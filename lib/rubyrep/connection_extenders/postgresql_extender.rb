# A cursor to iterate over the records returned by select_cursor.
# Only one row is kept in memory at a time.
class PGresult
  # Keep the database connection for access to the PostgreSQL adapter type conversion functions.
  attr_accessor :connection
  
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
      
      # Arndt Lehmann 2007-11-10: 
      # I don't fully understand this type conversion section.
      # However for compatibility reasons copied this time conversion part over
      # from PostgreSQLAdapter#select.
      # Would be better if this section would actually be verified.
      # For now I just copied and created according spec to ensure it doesn't go
      # totally haywire
      case self.type(field_index)
      when ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::BYTEA_COLUMN_TYPE_OID
        value = connection.unescape_bytea(value)
      when ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::TIMESTAMPTZOID, 
          ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::TIMESTAMPOID
        value = connection.cast_to_time(value)
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
      
      def self.included(mod)
        # calling mod.send or mod.class.send didn't work 
        # (at least during spec runs I got some rspec object / class back insead)
        ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.send :public, :cast_to_time, :unescape_bytea
      end
      # Executes the given sql query with the otional name written in the 
      # ActiveRecord log file.
      # Returns the results as a Cursor object supporting
      #   * next? - returns true if there are more rows to read
      #   * next_row - returns the row as a column => value hash and moves the cursor to the next row
      def select_cursor(sql, name = nil)
        result = execute sql, name
        result.connection = self
        result
      end
      
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

