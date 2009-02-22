# A cursor to iterate over the records returned by select_cursor.
# Only one row is kept in memory at a time.

module MysqlResultExtender
  # Returns true if there are more rows to read.
  def next?
    @current_row_num ||= 0
    @num_rows ||= self.num_rows()
    @current_row_num < @num_rows
  end
  
  # Returns the row as a column => value hash and moves the cursor to the next row.
  def next_row
    raise("no more rows available") unless next?
    row = fetch_hash()
    @current_row_num += 1
    row
  end
  
  # Releases the database resources hold by this cursor
  def clear
    free
  end
end

module RR

  # Overwrites #select_cursor to allow fetching of MySQL results in chunks
  class ProxyConnection

    # Allow selecting of MySQL results in chunks.
    # For full documentation of method interface refer to ProxyConnection#select_cursor.
    def select_cursor_with_mysql_chunks(options)
      if config[:adapter] != 'mysql' or !options.include?(:row_buffer_size) or options.include?(:query)
        select_cursor_without_mysql_chunks options
      else
        ConnectionExtenders::MysqlFetcher.new(self, options)
      end
    end
    alias_method_chain :select_cursor, :mysql_chunks unless method_defined?(:select_cursor_without_mysql_chunks)

  end

  module ConnectionExtenders

    # Fetches MySQL results in chunks
    class MysqlFetcher

      # The current database ProxyConnection
      attr_accessor :connection

      # hash of select options
      attr_accessor :options

      # column_name => value hash of the last returned row
      attr_accessor :last_row

      # Creates a new fetcher.
      # * +connection+: the current database connection
      # * +cursor_name+: name of the cursor from which to fetch
      # * +row_buffer_size+: number of records to read at once
      def initialize(connection, options)
        self.connection = connection
        self.options = options.clone
      end

      # Returns +true+ if there are more rows to read.
      def next?
        unless @current_result
          if last_row
            options.merge! :from => last_row, :exclude_starting_row => true
          end
          options[:query] = 
            connection.table_select_query(options[:table], options) +
            " limit #{options[:row_buffer_size]}"
          @current_result = connection.select_cursor_without_mysql_chunks(options)
        end
        @current_result.next?
      end

      # Returns the row as a column => value hash and moves the cursor to the next row.
      def next_row
        raise("no more rows available") unless next?
        self.last_row = @current_result.next_row
        unless @current_result.next?
          @current_result.clear
          @current_result = nil
        end
        self.last_row
      end

      # Closes the cursor and frees up all ressources
      def clear
        if @current_result
          @current_result.clear
          @current_result = nil
        end
      end
    end

    # Provides various MySQL specific functionality required by Rubyrep.
    module MysqlExtender
      RR::ConnectionExtenders.register :mysql => self

      # Executes the given sql query with the optional name written in the 
      # ActiveRecord log file.
      # :+row_buffer_size+ is not currently used.
      # Returns the results as a Cursor object supporting
      #   * next? - returns true if there are more rows to read
      #   * next_row - returns the row as a column => value hash and moves the cursor to the next row
      #   * clear - clearing the cursor (making allocated memory available for GC)
      def select_cursor(sql, row_buffer_size = 1000)
        result = execute sql
        result.send :extend, MysqlResultExtender
        result
      end
      
      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        row = self.select_one(<<-end_sql)
          select table_name from information_schema.tables 
          where table_schema = database() and table_name = '#{table}'
        end_sql
        if row.nil?
          raise "table '#{table}' does not exist"
        end
        
        rows = self.select_all(<<-end_sql)
          select column_name from information_schema.key_column_usage
          where table_schema = database() and table_name = '#{table}' 
          and constraint_name = 'PRIMARY'
          order by ordinal_position
        end_sql

        columns = rows.map {|_row| _row['column_name']}
        columns
      end

      # Returns for each given table, which other tables it references via
      # foreign key constraints.
      # * tables: an array of table names
      # Returns: a hash with
      # * key: name of the referencing table
      # * value: an array of names of referenced tables
      def referenced_tables(tables)
        rows = self.select_all(<<-end_sql)
          select distinct table_name as referencing_table, referenced_table_name as referenced_table
          from information_schema.key_column_usage
          where table_schema = database()
          and table_name in ('#{tables.join("', '")}')
        end_sql
        result = {}
        rows.each do |row|
          unless result.include? row['referencing_table']
            result[row['referencing_table']] = []
          end
          if row['referenced_table'] != nil
            result[row['referencing_table']] << row['referenced_table']
          end
        end
        tables.each do |table|
          result[table] = [] unless result.include? table
        end
        result
      end
    end
  end
end
