module RR
  
  # Makes management of logged change loaders easier
  class LoggedChangeLoaders

    # The current Session
    attr_accessor :session

    # A hash of LoggedChangeLoader instances for the :+left+ and :+right+ database
    attr_accessor :loaders

    # Create new logged change loaders.
    # * +session+: Current Session
    def initialize(session)
      self.session = session
      self.loaders = {}
      [:left, :right].each do |database|
        loaders[database] = LoggedChangeLoader.new(session, database)
      end
    end

    # Returns the LoggedChangeLoader for the specified (:+left+ or :+right+)
    # database.
    def [](database)
      loaders[database]
    end

    # Forces an update of the change log cache
    def update
      [:left, :right].each {|database| self[database].update :forced => true}
    end
  end

  # Caches the entries in the change log table
  class LoggedChangeLoader

    # The current +Session+.
    attr_accessor :session

    # Current database (either :+left+ or :+right+)
    attr_accessor :database

    # The current +ProxyConnection+.
    attr_accessor :connection

    # Index to the next unprocessed change in the +change_array+.
    attr_accessor :current_index

    # ID of the last cached change log record.
    attr_accessor :current_id

    # Array with all cached changes.
    # Processed change log records are replaced with +nil+.
    attr_accessor :change_array

    # Tree (hash) structure for fast access to all cached changes.
    # First level of tree:
    # * key: table name
    # * value: 2nd level tree
    # 2nd level tree:
    # * key: the change_key value of the according change log records.
    # * value:
    #   An array of according change log records (column_name => value hash).
    #   Additional entry of each change log hash:
    #   * key: 'array_index'
    #   * value: index to the change log record in +change_array+
    attr_accessor :change_tree

    # Date of last update of the cache
    attr_accessor :last_updated

    # Initializes / resets the cache.
    def init_cache
      self.change_tree = {}
      self.change_array = []
      self.current_index = 0
    end
    private :init_cache

    # Create a new change log record cache.
    # * +session+: The current +Session+
    # * +database+: Either :+left+ or :+right+
    def initialize(session, database)
      self.session = session
      self.database = database
      self.connection = session.send(database)

      init_cache
      self.current_id = -1
      self.last_updated = 1.year.ago
    end

    # Updates the cache.
    # Options is a hash determining when the update is actually executed:
    # * :+expire_time+: cache is older than the given number of seconds
    # * :+forced+: if +true+ update the cache even if not yet expired
    def update(options = {:forced => false, :expire_time => 1})
      return unless options[:forced] or Time.now - self.last_updated >= options[:expire_time]
      
      self.last_updated = Time.now

      # First, let's use a LIMIT clause (via :row_buffer_size option) to verify
      # if there are any pending changes.
      # (If there are many pending changes, this is (at least with PostgreSQL)
      # much faster.)
      cursor = connection.select_cursor(
        :table => change_log_table,
        :from => {'id' => current_id},
        :exclude_starting_row => true,
        :row_buffer_size => 1
      )
      return unless cursor.next?

      # Something is here. Let's actually load it.
      cursor = connection.select_cursor(
        :table => change_log_table,
        :from => {'id' => current_id},
        :exclude_starting_row => true,
        :type_cast => true,
        :row_buffer_size => session.configuration.options[:row_buffer_size]
      )
      while cursor.next?
        change = cursor.next_row
        self.current_id = change['id']
        self.change_array << change
        change['array_index'] = self.change_array.size - 1

        table_change_tree = change_tree[change['change_table']] ||= {}
        key_changes = table_change_tree[change['change_key']] ||= []
        key_changes << change
      end
      cursor.clear
    end

    # Returns the creation time of the oldest unprocessed change log record.
    def oldest_change_time
      change = oldest_change
      change['change_time'] if change
    end

    # Returns the oldest unprocessed change log record (column_name => value hash).
    def oldest_change
      update
      oldest_change = nil
      unless change_array.empty?
        while (oldest_change = change_array[self.current_index]) == nil
          self.current_index += 1
        end
      end
      oldest_change
    end

    # Returns the specified change log record (column_name => value hash).
    # * +change_table+: the name of the table that was changed
    # * +change_key+: the change key of the modified record
    def load(change_table, change_key)
      update
      change = nil
      table_change_tree = change_tree[change_table]
      if table_change_tree
        key_changes = table_change_tree[change_key]
        if key_changes
          # get change object and delete from key_changes
          change = key_changes.shift

          # delete change from change_array
          change_array[change['array_index']] = nil

          # delete change from database
          connection.execute "delete from #{change_log_table} where id = #{change['id']}"

          # delete key_changes if empty
          if key_changes.empty?
            table_change_tree.delete change_key
          end

          # delete table_change_tree if empty
          if table_change_tree.empty?
            change_tree.delete change_table
          end

          # reset everything if no more changes remain
          if change_tree.empty?
            init_cache
          end
        end
      end
      change
    end

    # Returns the name of the change log table
    def change_log_table
      @change_log_table ||= "#{session.configuration.options[:rep_prefix]}_pending_changes"
    end
    private :change_log_table
  end
end
