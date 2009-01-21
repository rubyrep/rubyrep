$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'
require 'forwardable'

require 'active_record/connection_adapters/abstract_adapter'

module RR

  # This class represents a remote activerecord database connection.
  # Normally created by DatabaseProxy
  class ProxyConnection
    extend Forwardable
    
    # The database connection
    attr_accessor :connection

    # A hash as described by ActiveRecord::Base#establish_connection
    attr_accessor :config
    
    # Forward certain methods to the proxied database connection
    def_delegators \
      :connection, :columns, :quote_column_name,
      :quote_table_name, :select_cursor, :execute,
      :select_one, :select_all, :tables,
      :begin_db_transaction, :rollback_db_transaction, :commit_db_transaction,
      :referenced_tables,
      :create_or_replace_replication_trigger_function,
      :create_replication_trigger, :drop_replication_trigger, :replication_trigger_exists?,
      :outdated_sequence_values, :update_sequences, :clear_sequence_setup,
      :create_table, :drop_table, :add_big_primary_key
    
    # Caching the primary keys. This is a hash with
    #   * key: table name
    #   * value: array of primary key names
    attr_accessor :primary_key_names_cache
    
    # Hash to register cursors.
    # Purpose:
    #   Objects only referenced remotely via DRb can be garbage collected.
    #   We register them in this hash to protect them from unintended garbage collection.
    attr_accessor :cursors
    
    # 2-level Hash of table_name => column_name => Column objects.
    attr_accessor :table_columns
    
    # Hash of table_name => array of column names pairs.
    attr_accessor :table_column_names

    # A hash of manually overwritten primary keys:
    # * key: table_name
    # * value: array of primary key names
    attr_accessor :manual_primary_keys
    
    # Returns an array of primary key names for the given +table_name+.
    # Caches the result for future calls. Allows manual overwrites through
    # the Configuration options +:primary_key_names+ or :+primary_key_only_limit+.
    #
    # Parameters:
    # * +table_name+: name of the table
    # * +options+: An option hash with the following valid options:
    #   * :+raw+: if +true+, than don't use manual overwrites and don't cache
    def primary_key_names(table_name, options = {})
      return connection.primary_key_names(table_name) if options[:raw]
      
      self.primary_key_names_cache ||= {}
      result = primary_key_names_cache[table_name]
      unless result
        result = manual_primary_keys[table_name] || connection.primary_key_names(table_name)
        primary_key_names_cache[table_name] = result
      end
      result
    end
    
    # Returns a Hash of currently registerred cursors
    def cursors
      @cursors ||= {}
    end
    
    # Store a cursor in the register to protect it from the garbage collector.
    def save_cursor(cursor)
      cursors[cursor] = cursor
    end
    
    # Create a session on the proxy side according to provided configuration hash.
    # +config+ is a hash as described by ActiveRecord::Base#establish_connection
    def initialize(config)
      self.connection = ConnectionExtenders.db_connect config
      self.config = config
      self.manual_primary_keys = {}
    end

    # Checks if the connection is still active and if not, reestablished it.
    def refresh
      unless self.connection.active?
        self.connection = ConnectionExtenders.db_connect config
      end
    end
    
    # Destroys the session
    def destroy
      self.connection.disconnect!
      
      cursors.each_key do |cursor|
        cursor.destroy
      end
      cursors.clear
    end
    
    # Quotes the given value. It is assumed that the value belongs to the specified column name and table name.
    # Caches the column objects for higher speed.
    def quote_value(table, column, value)
      self.table_columns ||= {}
      unless table_columns.include? table
        table_columns[table] = {}
        columns(table).each {|c| table_columns[table][c.name] = c}
      end
      connection.quote value, table_columns[table][column]
    end
    
    # Create a cursor for the given table.
    #   * +cursor_class+: should specify the Cursor class (e. g. ProxyBlockCursor or ProxyRowCursor).
    #   * +table+: name of the table 
    #   * +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def create_cursor(cursor_class, table, options = {})
      cursor = cursor_class.new self, table
      cursor.prepare_fetch options
      save_cursor cursor
      cursor
    end
    
    # Destroys the provided cursor and removes it from the register
    def destroy_cursor(cursor)
      cursor.destroy
      cursors.delete cursor
    end
    
    # Returns an array of column names of the given table name.
    # The array is ordered in the sequence as returned by the database.
    # The result is cached for higher speed.
    def column_names(table)
      self.table_column_names ||= {}
      unless table_column_names.include? table
        table_column_names[table] = columns(table).map {|c| c.name}
      end
      table_column_names[table]
    end
  
    # Returns a list of quoted column names for the given +table+ as comma 
    # separated string.
    def quote_column_list(table)
      column_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_column_list
    
    # Returns a list of quoted primary key names for the given +table+ as comma
    # separated string.
    def quote_key_list(table)
      primary_key_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_key_list
    
    
    # Generates an sql condition string for the given +table+ based on
    #   * +row+: a hash of primary key => value pairs designating the target row
    #   * +condition+: the type of sql condition (something like '>=' or '=', etc.)
    def row_condition(table, row, condition)
      query_part = ""
      query_part << ' (' << quote_key_list(table) << ') ' << condition
      query_part << ' (' << primary_key_names(table).map do |key|
        quote_value(table, key, row[key])
      end.join(', ') << ')'
      query_part
    end
    private :row_condition

    # Returns an SQL query string for the given +table+ based on the provided +options+.
    # +options+ is a hash that can contain any of the following:
    #   * +:from+: nil OR the hash of primary key => value pairs designating the start of the selection
    #   * +:to+: nil OR the hash of primary key => value pairs designating the end of the selection
    #   * +:row_keys+: an array of primary key => value hashes specify the target rows.
    def table_select_query(table, options = {})
      options.each_key do |key| 
        raise "options must only include :from, :to or :row_keys" unless [:from, :to, :row_keys].include? key
      end
      query = "select #{quote_column_list(table)}"
      query << " from #{quote_table_name(table)}"
      query << " where" unless options.empty?
      first_condition = true
      if options[:from]
        first_condition = false
        query << row_condition(table, options[:from], '>=')
      end
      if options[:to]
        query << ' and' unless first_condition
        first_condition = false
        query << row_condition(table, options[:to], '<=')
      end
      if options[:row_keys]
        query << ' and' unless first_condition
        if options[:row_keys].empty?
          query << ' false'
        else
          query << ' (' << quote_key_list(table) << ') in ('
          first_key = true
          options[:row_keys].each do |row|
            query << ', ' unless first_key
            first_key = false
            query << '(' << primary_key_names(table).map do |key|
              quote_value(table, key, row[key])
            end.join(', ') << ')'
          end
          query << ')'
        end
      end
      query << " order by #{quote_key_list(table)}"

      query
    end
    
    # Returns an SQL insert query for the given +table+ and +values+.
    # +values+ is a hash of column_name => value pairs.
    def table_insert_query(table, values)
      query = "insert into #{quote_table_name(table)}"
      query << '(' << values.keys.map do |column_name|
        quote_column_name(column_name)
      end.join(', ') << ') '
      query << 'values(' << values.map do |column_name, value|
        quote_value(table, column_name, value)
      end.join(', ') << ')'
      query
    end
    
    # Inserts the specified records into the named +table+.
    # +values+ is a hash of column_name => value pairs.
    def insert_record(table, values)
      execute table_insert_query(table, values)
    end
    
    # Returns an SQL update query.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    def table_update_query(table, values, org_key = nil)
      org_key ||= values
      query = "update #{quote_table_name(table)} set "
      query << values.map do |column_name, value|
        "#{quote_column_name(column_name)} = #{quote_value(table, column_name, value)}"
      end.join(', ')
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, org_key[key])
      end.join(', ') << ")"
    end
    
    # Updates the specified records of the specified table.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs.
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    def update_record(table, values, org_key = nil)
      execute table_update_query(table, values, org_key)
    end

    # Returns an SQL delete query for the given +table+ and +values+
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    def table_delete_query(table, values)
      query = "delete from #{quote_table_name(table)}"
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, values[key])
      end.join(', ') << ")"
    end
    
    # Deletes the specified record from the named +table+.
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    def delete_record(table, values)
      execute table_delete_query(table, values)
    end
  end
end
