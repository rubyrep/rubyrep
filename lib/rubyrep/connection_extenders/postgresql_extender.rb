require 'time'

# Hack:
# For some reasons these methods were removed in Rails 2.2.2, thus breaking
# the binary and multi-lingual data loading.
# So here they are again.
module ActiveRecord
  module ConnectionAdapters
    # PostgreSQL-specific extensions to column definitions in a table.
    class PostgreSQLColumn < Column #:nodoc:

      # Escapes binary strings for bytea input to the database.
      def self.string_to_binary(value)
        if PGconn.respond_to?(:escape_bytea)
          self.class.module_eval do
            define_method(:string_to_binary) do |value|
              PGconn.escape_bytea(value) if value
            end
          end
        else
          self.class.module_eval do
            define_method(:string_to_binary) do |value|
              if value
                result = ''
                value.each_byte { |c| result << sprintf('\\\\%03o', c) }
                result
              end
            end
          end
        end
        self.class.string_to_binary(value)
      end

      # Unescapes bytea output from a database to the binary string it represents.
      def self.binary_to_string(value)
        # In each case, check if the value actually is escaped PostgreSQL bytea output
        # or an unescaped Active Record attribute that was just written.
        if PGconn.respond_to?(:unescape_bytea)
          self.class.module_eval do
            define_method(:binary_to_string) do |value|
              if value =~ /\\\d{3}/
                PGconn.unescape_bytea(value)
              else
                value
              end
            end
          end
        else
          self.class.module_eval do
            define_method(:binary_to_string) do |value|
              if value =~ /\\\d{3}/
                result = ''
                i, max = 0, value.size
                while i < max
                  char = value[i]
                  if char == ?\\
                    if value[i+1] == ?\\
                      char = ?\\
                      i += 1
                    else
                      char = value[i+1..i+3].oct
                      i += 3
                    end
                  end
                  result << char
                  i += 1
                end
                result
              else
                value
              end
            end
          end
        end
        self.class.binary_to_string(value)
      end
    end
  end
end

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

      # Disables schema extraction from table names by overwriting the according
      # ActiveRecord method.
      # Necessary to support table names containing dots (".").
      # (This is possible as rubyrep exclusively uses the search_path setting to
      # support PostgreSQL schemas.)
      def extract_pg_identifier_from_name(name)
        return name, nil
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
        column_ids = column_parray.sub(/^\{(.*)\}$/,'\1').split(',').map {|a| a.to_i}

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

      # Sets the schema search path as per configuration parameters
      def initialize_search_path
        execute "SET search_path TO #{config[:schema_search_path] || 'public'}"
      end

      # *** Moneky patch***
      # Returns the column objects for the named table.
      # Fixes JRuby schema support
      def columns(table_name, name = nil)
        jdbc_connection = @connection.connection # the actual JDBC DatabaseConnection
        @unquoted_schema ||= select_one("show search_path")['search_path']

        # check if table exists
        table_results = jdbc_connection.meta_data.get_tables(
          jdbc_connection.catalog,
          @unquoted_schema,
          table_name,
          ["TABLE","VIEW","SYNONYM"].to_java(:string)
        )
        table_exists = table_results.next
        table_results.close
        raise "table '#{table_name}' not found" unless table_exists

        # get ResultSet for columns of table
        column_results = jdbc_connection.meta_data.get_columns(
          jdbc_connection.catalog,
          @unquoted_schema,
          table_name,
          nil
        )
        
        # create the Column objects
        columns = []
        while column_results.next
          
          # generate type clause
          type_clause = column_results.get_string('TYPE_NAME')
          precision = column_results.get_int('COLUMN_SIZE')
          scale = column_results.get_int('DECIMAL_DIGITS')
          if precision > 0
            type_clause += "(#{precision}#{scale > 0 ? ",#{scale}" : ""})"
          end

          # create column
          columns << ::ActiveRecord::ConnectionAdapters::JdbcColumn.new(
            @config,
            column_results.get_string('COLUMN_NAME'),
            column_results.get_string('COLUMN_DEF'),
            type_clause,
            column_results.get_string('IS_NULLABLE').strip == "NO"
          )
        end
        column_results.close
        
        columns
      end if RUBY_PLATFORM =~ /java/

      # *** Monkey patch***
      # Returns the list of a table's column names, data types, and default values.
      # This overwrites the according ActiveRecord::PostgreSQLAdapter method
      # to
      # * work with tables containing a dot (".") and
      # * only look for tables in the current schema search path.
      def column_definitions(table_name) #:nodoc:
        rows = self.select_all <<-end_sql
          SELECT
            a.attname as name,
            format_type(a.atttypid, a.atttypmod) as type,
            d.adsrc as source,
            a.attnotnull as notnull
          FROM pg_attribute a LEFT JOIN pg_attrdef d
            ON a.attrelid = d.adrelid AND a.attnum = d.adnum
          WHERE a.attrelid = (
            SELECT oid FROM pg_class
            WHERE relname = '#{table_name}' AND relnamespace IN
              (SELECT oid FROM pg_namespace WHERE nspname in (#{schemas}))
            LIMIT 1
            )
            AND a.attnum > 0 AND NOT a.attisdropped
          ORDER BY a.attnum
        end_sql
    
        rows.map {|row| [row['name'], row['type'], row['source'], row['notnull']]}
      end

    end
  end
end

