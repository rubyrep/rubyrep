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
  module ConnectionExtenders

    # Provides various MySQL specific functionality required by Rubyrep.
    module MysqlExtender
      RR::ConnectionExtenders.register :mysql => self

      # Executes the given sql query with the optional name written in the 
      # ActiveRecord log file.
      # Returns the results as a Cursor object supporting
      #   * next? - returns true if there are more rows to read
      #   * next_row - returns the row as a column => value hash and moves the cursor to the next row
      #   * clear - clearing the cursor (making allocated memory available for GC)
      def select_cursor(sql, name = nil)
        result = execute sql, name
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
          raise "table does not exist"
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

    end
  end
end
