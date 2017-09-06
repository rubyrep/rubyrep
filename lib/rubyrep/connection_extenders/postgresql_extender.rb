require 'time'

module RR
  module ConnectionExtenders

    # Provides various PostgreSQL specific functionality required by Rubyrep.
    module PostgreSQLExtender
      RR::ConnectionExtenders.register :postgresql => self
      
      # Returns an array of schemas in the current search path.
      def schemas
        unless @schemas
          search_path = select_one("show search_path")['search_path']
          @schemas = search_path.split(/,/).map { |p| quote(p.strip) }.join(',')
        end
        @schemas
      end

      # *** Monkey patch***
      # Returns the list of all tables in the schema search path or a specified schema.
      # This overwrites the according ActiveRecord::PostgreSQLAdapter method
      # to make sure that also search paths with spaces work
      # (E. g. 'public, rr' instead of only 'public,rr')
      def tables(name = nil)
        select_all(<<-SQL, name).map { |row| row['tablename'] }
          SELECT tablename
            FROM pg_tables
           WHERE schemaname IN (#{schemas})
        SQL
      end

      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        row = self.select_one(<<-end_sql)
          SELECT relname
          FROM pg_class
          WHERE relname = '#{table}' and relnamespace IN
            (SELECT oid FROM pg_namespace WHERE nspname in (#{schemas}))
        end_sql
        raise "table '#{table}' does not exist" if row.nil?
        
        row = self.select_one(<<-end_sql)
          SELECT cons.conkey 
          FROM pg_class           rel
          JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
          WHERE cons.contype = 'p' AND rel.relname = '#{table}' AND rel.relnamespace IN
            (SELECT oid FROM pg_namespace WHERE nspname in (#{schemas}))
        end_sql
        return [] if row.nil?
        column_parray = row['conkey']
        
        # Change a Postgres Array of attribute numbers
        # (returned in String form, e. g.: "{1,2}") into an array of Integers
        if column_parray.kind_of?(Array)
          column_ids = column_parray # in JRuby the attribute numbers are already returned as array
        else
          column_ids = column_parray.sub(/^\{(.*)\}$/,'\1').split(',').map {|a| a.to_i}
        end

        columns = {}
        rows = self.select_all(<<-end_sql)
          SELECT attnum, attname
          FROM pg_class           rel
          JOIN pg_constraint      cons ON (rel.oid = cons.conrelid)
          JOIN pg_attribute       attr ON (rel.oid = attr.attrelid and attr.attnum = any (cons.conkey))
          WHERE cons.contype = 'p' AND rel.relname = '#{table}' AND rel.relnamespace IN
            (SELECT oid FROM pg_namespace WHERE nspname in (#{schemas}))
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
          and referencing.relnamespace IN
            (SELECT oid FROM pg_namespace WHERE nspname in (#{schemas}))
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

      # Quotes the value so it can be used in SQL insert / update statements.
      #
      # @param [Object] value the target value
      # @param [ActiveRecord::ConnectionAdapters::PostgreSQLColumn] column the target column
      # @return [String] the quoted string
      def column_aware_quote(value, column)
        quote column.type_cast_for_database value
      end

      # Casts a value returned from the database back into the according ruby type.
      #
      # @param [Object] value the received value
      # @param [ActiveRecord::ConnectionAdapters::PostgreSQLColumn] column the originating column
      # @return [Object] the casted value
      def fixed_type_cast(value, column)
        if column.sql_type == 'bytea' and RUBY_PLATFORM == 'java'
          # Apparently in Java / JRuby binary data are automatically unescaped.
          # So #type_cast_from_database must be prevented from double-unescaping the binary data.
            value
        else
          column.type_cast_from_database value
        end
      end

    end
  end
end

