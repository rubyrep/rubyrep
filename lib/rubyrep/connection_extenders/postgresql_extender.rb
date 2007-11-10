# A cursor to iterate over the records returned by select_cursor.
# Only one row is kept in memory at a time.
class PGresult
  
  # Returns true if there are more rows to read.
  def next?
    @current_row_num ||= 0
    @num_rows ||= self.num_tuples()
    @current_row_num < @num_rows
  end
  
  # Returns the row as a column => value hash and moves the cursor to the next row.
  def next_row
    raise("no more rows available") unless next?
    row = {}
    @fields ||= self.fields
    @fields.each_with_index do |field, field_index| 
      value = self.getvalue @current_row_num, field_index
      
      case self.type(field_index)
      when ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::BYTEA_COLUMN_TYPE_OID
        value = unescape_bytea(value)
      when ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::TIMESTAMPTZOID, 
          ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::TIMESTAMPOID
        value = cast_to_time(value)
      when ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::NUMERIC_COLUMN_TYPE_OID
        value = value.to_d if value.respond_to?(:to_d)
      end
      
      row[@fields[field_index]] = value
    end
    @current_row_num += 1
    row
  end
end

module RR
  module ConnectionExtenders

    # Provides various PostgreSQL specific functionality required by Rubyrep.
    module PostgreSQLExtender
      RR::ConnectionExtenders.register :postgresql => self
      
      # Executes the given sql query with the otional name written in the 
      # ActiveRecord log file.
      # Returns the results as a Cursor object supporting
      #   * next? - returns true if there are more rows to read
      #   * next_row - returns the row as a column => value hash and moves the cursor to the next row
      def select_cursor(sql, name = nil)
	execute sql, name
      end
      
      def select2(sql, name = nil) select(sql,name) end
      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
	row = self.select_one(<<-end_sql)
	  SELECT relname
	  FROM pg_class
	  WHERE relname = '#{table}'
        end_sql
        if row.nil?
	  raise "table does not exist"
	end
	
	row = self.select_one(<<-end_sql)
	  SELECT cons.conkey 
	  FROM pg_class           rel
	  JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
	  WHERE cons.contype = 'p' AND rel.relname = '#{table}'	  
        end_sql
	if row.nil?
	  return []
	end
	column_parray = row['conkey']
	
        # Change a Postgres Array of attribute numbers 
        # (returned in String form, e. g.: "{1,2}") into an array of Integers
	column_ids = column_parray.sub(/^\{(.*)\}$/,'\1').split(',').map {|a| a.to_i}
        
        columns = {}
        rows = self.select_all(<<-end_sql)
          SELECT attnum, attname 
          FROM pg_class           rel
          JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
          JOIN pg_attribute       attr ON (rel.oid = attr.attrelid and attr.attnum = any (cons.conkey))
          WHERE cons.contype = 'p' AND rel.relname = '#{table}'
        end_sql
	sorted_columns = []
	if not rows.nil?
	  rows.each() {|row| columns[row['attnum'].to_i] = row['attname']}
          sorted_columns = column_ids.map {|column_id| columns[column_id]}
	end
	sorted_columns
      end
      
    end
  end
end

