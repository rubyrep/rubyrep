module RR
  module ConnectionExtenders

    # Provides various PostgreSQL specific functionality required by Rubyrep.
    module PostgreSQLExtender
      RR::ConnectionExtenders.register :postgresql => self
      
      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
	column_parray = self.select_one(<<-end_sql)['conkey']
	  SELECT cons.conkey 
	  FROM pg_class           rel
	  JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
	  WHERE cons.contype = 'p' AND rel.relname = '#{table}'	  
        end_sql
        # Change a Postgres Array of attribute numbers 
        # (returned in String form, e. g.: "{1,2}") into an array of Integers
	column_ids = column_parray.sub(/^\{(.*)\}$/,'\1').split(',').map {|a| a.to_i}
        
        columns = {}
        self.select_all(<<-end_sql).each() {|row| columns[row['attnum'].to_i] = row['attname']}
          SELECT attnum, attname 
          FROM pg_class           rel
          JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
          JOIN pg_attribute       attr ON (rel.oid = attr.attrelid and attr.attnum = any (cons.conkey))
          WHERE cons.contype = 'p' AND rel.relname = '#{table}'
        end_sql
        sorted_columns = column_ids.map {|column_id| columns[column_id]}
      end
      
    end
  end
end

