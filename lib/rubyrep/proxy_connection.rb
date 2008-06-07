$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'
require 'forwardable'

module RR

  # This class represents a remote activerecord database connection.
  # Normally created by DatabaseProxy
  class ProxyConnection
    extend Forwardable
    
    # The database connection
    attr_accessor :connection
    
    # Forward certain methods to the proxied database connection
    def_delegators \
      :connection, :columns, :quote_column_name, \
      :quote_table_name, :select_cursor, :execute, \
      :select_one, :tables, \
      :begin_db_transaction, :rollback_db_transaction, :commit_db_transaction
    
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
    
    # Returns an array of primary key names for the given +table_name+.
    # Caches the result for future calls.
    def primary_key_names(table_name)
      self.primary_key_names_cache ||= {}
      result = primary_key_names_cache[table_name]
      unless result
        result = primary_key_names_cache[table_name] = connection.primary_key_names(table_name)
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
        connection.quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_column_list
    
    # Returns a quoted and comma separated list of primary key names for the 
    # given +table+.
    def quote_key_list(table)
      primary_key_names(table).map do |column_name| 
        connection.quote_column_name(column_name)
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
  end
end
