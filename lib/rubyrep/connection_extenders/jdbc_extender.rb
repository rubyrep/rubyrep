require 'java'
include_class 'java.sql.Types'

module RR
  module ConnectionExtenders

    # Provides various JDBC specific functionality required by Rubyrep.
    module JdbcSQLExtender
      RR::ConnectionExtenders.register :jdbc => self
      
      # A cursor to iterate over the records returned by select_cursor.
      # Only one row is kept in memory at a time.
      module JdbcResultSet
        # Returns true if there are more rows to read.
        def next?
          if @next_status == nil
            @next_status = self.next
          end
          @next_status
        end
        
        # Returns the row as a column => value hash and moves the cursor to the next row.
        def next_row
          raise("no more rows available") unless next?
          @next_status = nil

          unless @columns
            meta_data = self.getMetaData
            stores_upper = self.getStatement.getConnection.getMetaData.storesUpperCaseIdentifiers
            column_count = meta_data.getColumnCount
            @columns = Array.new(column_count)
            @columns.each_index do |i|
              column_name = meta_data.getColumnName(i+1)
              if stores_upper and not column_name =~ /[a-z]/
                column_name.downcase!
              end
              @columns[i] = {
                :index => i+1,
                :name => column_name,
                :type => meta_data.getColumnType(i+1)
                #:scale => meta_data.getScale(i+1)
              }
            end
          end

          row = {}
          @columns.each_index do |i|
            row[@columns[i][:name]] = jdbc_to_ruby(@columns[i])
          end

          row
        end
        
        # Releases the databases resources hold by this cursor
        def clear
          @columns = nil
          self.close
        end
        
        # Converts the specified column of the current row to the proper ruby string
        # column is a hash with the following elements:
        #   * :index: field number (starting with 1) of the result set field
        #   * :type: the java.sql.Type constant specifying the type of the result set field
        def jdbc_to_ruby(column)
          case column[:type]
          when Types::BINARY, Types::BLOB, Types::LONGVARBINARY, Types::VARBINARY
            is = self.getBinaryStream(column[:index])
            if is == nil or self.wasNull
              return nil
            end
            byte_list = org.jruby.util.ByteList.new(2048)
            buffer = Java::byte[2048].new
            while (n = is.read(buffer)) != -1
              byte_list.append(buffer, 0, n)
            end
            is.close
            return byte_list.toString
          when Types::LONGVARCHAR, Types::CLOB
            rss = self.getCharacterStream(column[:index])
            if rss == nil or self.wasNull
              return nil
            end
            str = java.lang.StringBuffer.new(2048)
            cuf = Java::char[2048].new
            while (n = rss.read(cuf)) != -1
              str.append(cuf, 0, n)
            end
            rss.close
            return str.toString
          when Types::TIMESTAMP
            time = self.getTimestamp(column[:index]);
            if  time == nil or self.wasNull
              return nil
            end
            time_string = time.toString()
            time_string = time_string.gsub(/ 00:00:00.0$/, '')
            return time_string
          else
            value = self.getString(column[:index])
            if value == nil or self.wasNull
              return nil
            end
            return value
          end
        end
        private :jdbc_to_ruby
      end

      # Monkey patch for activerecord-jdbc-adapter-0.7.2 as it doesn't set the 
      # +@active+ flag to false, thus ActiveRecord#active? incorrectly confirms
      # the connection to still be active.
      def disconnect!
        super
        @active = false
      end

      # Executes the given sql query with the otional name written in the 
      # ActiveRecord log file.
      # Returns the results as a Cursor object supporting
      #   * next? - returns true if there are more rows to read
      #   * next_row - returns the row as a column => value hash and moves the cursor to the next row
      #   * clear - clearing the cursor (making allocated memory available for GC)
      def select_cursor(sql, name = nil)
        #result = execute sql, name
        #result
        #puts @connection.connection.methods.sort.to_yaml
        statement = @connection.connection.createStatement
        result_set = statement.executeQuery(sql)
        result_set.send :extend, JdbcResultSet
      end
      
      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        if not tables.include? table
          raise "table does not exist"
        end
        columns = []
        result_set = @connection.connection.getMetaData.getPrimaryKeys(nil, nil, table);
        while result_set.next
          column_name = result_set.getString("COLUMN_NAME")
          key_seq = result_set.getShort("KEY_SEQ")
          columns << {:column_name => column_name, :key_seq => key_seq}
        end
        columns.sort! {|a, b| a[:key_seq] <=> b[:key_seq]}
        key_names = columns.map {|column| column[:column_name]}
        key_names
      end
    end
  end
end