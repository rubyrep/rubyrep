# A cursor to iterate over the records returned by select_cursor.
# Only one row is kept in memory at a time.
class PGresult
  # Returns true if there are more rows to read.
  def next?
    @current_row_num ||= 0
    @num_rows ||= self.ntuples()
    @current_row_num < @num_rows
  end
  
  # Returns the row as a column => value hash and moves the cursor to the next row.
  def next_row
    raise("no more rows available") unless next?
    row = {}
    @fields ||= self.fields
    @fields.each_with_index do |field, field_index| 
      if self.getisnull(@current_row_num, field_index)
        value = nil
      else
        value = self.getvalue @current_row_num, field_index
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
      #   * clear - clearing the cursor (making allocated memory available for GC)
      def select_cursor(sql, name = nil)
        result = execute sql, name
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
          rows.each() {|r| columns[r['attnum'].to_i] = r['attname']}
          sorted_columns = column_ids.map {|column_id| columns[column_id]}
        end
        sorted_columns
      end

      # Returns for each given table, which other tables it references via
      # foreign key constraints.
      # * tables: an array of table names
      # Returns: a hash with
      # * key: name of the referencing table
      # * value: an array of names of referenced tables
      def referenced_tables(tables)
        rows = self.select_all(<<-end_sql)
          select distinct referencing.relname as referencing_table, referenced.relname as referenced_table
          from pg_class referencing
          left join pg_constraint on referencing.oid = pg_constraint.conrelid
          left join pg_class referenced on pg_constraint.confrelid = referenced.oid
          where referencing.relkind='r'
          and referencing.relname in ('#{tables.join("', '")}')
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
        result
      end
    end
  end
end

