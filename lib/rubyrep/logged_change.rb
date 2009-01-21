module RR
  
  class Session

    # Returns the +LoggedChangeLoader+ of the specified database.
    # * database: either :+left+ or :+right+
    def change_loader(database)
      @change_loaders ||= {}
      unless change_loader = @change_loaders[database]
        change_loader = @change_loaders[database] = LoggedChangeLoader.new(self, database)
      end
      change_loader
    end

    # Forces an update of the change log cache
    def reload_changes
      change_loader(:left).update :forced => true
      change_loader(:right).update :forced => true
    end

  end

  # Caches the entries in the change log table
  class LoggedChangeLoader

    # The current +Session+.
    attr_accessor :session

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
    #   The according change log record (column_name => value hash).
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

      org_cursor = connection.select_cursor(<<-end_sql)
        select * from #{change_log_table}
        where id > #{current_id}
        order by id
      end_sql
      cursor = TypeCastingCursor.new(connection,
        change_log_table, org_cursor)
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

  # Describes a single logged record change.
  # 
  # Note:
  # The change loading functionality depends on the current database session
  # being executed in an open database transaction.
  # Also at the end of change processing the transaction must be committed.
  class LoggedChange

    # The current Session
    attr_accessor :session

    # The database which was changed. Either :+left+ or :+right+.
    attr_accessor :database

    # The name of the changed table
    attr_accessor :table

    # When the first change to the record happened
    attr_accessor :first_changed_at

    # When the last change to the record happened
    attr_accessor :last_changed_at

    # Type of the change. Either :+insert+, :+update+ or :+delete+.
    attr_accessor :type

    # A column_name => value hash identifying the changed record
    attr_accessor :key

    # Only used for updates: a column_name => value hash of the original primary
    # key of the updated record
    attr_accessor :new_key

    # Creates a new LoggedChange instance.
    # * +session+: the current Session
    # * +database+: either :+left+ or :+right+
    def initialize(session, database)
      self.session = session
      self.database = database
      self.type = :no_change
    end

    # A hash describing how the change state morph based on newly found change
    # records.
    # * key: String consisting of 2 letters
    #   * first letter: describes current type change (nothing, insert, update, delete)
    #   * second letter: the new change type as read of the change log table
    # * value:
    #   The resulting change type.
    # [1]: such cases shouldn't happen. but just in case, choose the most
    # sensible solution.
    TYPE_CHANGES = {
      'NI' => 'I',
      'NU' => 'U',
      'ND' => 'D',
      'II' => 'I', # [1]
      'IU' => 'I',
      'ID' => 'N',
      'UI' => 'U', # [1]
      'UU' => 'U',
      'UD' => 'D',
      'DI' => 'U',
      'DU' => 'U', # [1]
      'DD' => 'D', # [1]
    }

    # A hash translating the short 1-letter types to the according symbols
    SHORT_TYPES = {
      'I' => :insert,
      'U' => :update,
      'D' => :delete,
      'N' => :no_change
    }
    # A hash translating the symbold types to according 1 letter types
    LONG_TYPES = SHORT_TYPES.invert

    # Returns the configured key separator
    def key_sep
      @key_sep ||= session.configuration.options[:key_sep]
    end

    # Returns a column_name => value hash based on the provided +raw_key+ string
    # (which is a string in the format as read directly from the change log table).
    def key_to_hash(raw_key)
      result = {}
      #raw_key.split(key_sep).each_slice(2) {|a| result[a[0]] = a[1]}
      raw_key.split(key_sep).each_slice(2) {|field_name, value| result[field_name] = value}
      result
    end

    # Loads the change as per #table and #key. Works if the LoggedChange instance
    # is totally new or was already loaded before.
    def load
      current_type = LONG_TYPES[type]

      org_key = new_key || key
      # change to key string as can be found in change log table
      org_key = session.send(database).primary_key_names(table).map do |key_name|
        "#{key_name}#{key_sep}#{org_key[key_name]}"
      end.join(key_sep)
      current_key = org_key

      while change = session.change_loader(database).load(table, current_key)

        new_type = change['change_type']
        current_type = TYPE_CHANGES["#{current_type}#{new_type}"]

        self.first_changed_at ||= change['change_time']
        self.last_changed_at = change['change_time']

        if change['change_type'] == 'U' and change['change_new_key'] != current_key
          current_key = change['change_new_key']
        end
      end

      self.type = SHORT_TYPES[current_type]
      self.new_key = nil
      if type == :update
        self.key ||= key_to_hash(org_key)
        self.new_key = key_to_hash(current_key)
      else
        self.key = key_to_hash(current_key)
      end
    end

    # Loads the change with the specified key for the named +table+.
    # * +table+: name of the table
    # * +key+: a column_name => value hash for all primary key columns of the table
    def load_specified(table, key)
      self.table = table
      self.key = key
      load
    end

    # Returns the time of the oldest change. Returns +nil+ if there are no
    # changes left.
    def oldest_change_time
      session.change_loader(database).oldest_change_time
    end

    # Loads the oldest available change
    def load_oldest
      begin
        change = session.change_loader(database).oldest_change
        break unless change
        self.key = key_to_hash(change['change_key'])
        self.table = change['change_table']
        load
      end until type != :no_change
    end

    # Prevents session from going into YAML output
    def to_yaml_properties
      instance_variables.sort.reject {|var_name| var_name == '@session'}
    end

  end
end