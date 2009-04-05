require 'java'

module RR
  module ConnectionExtenders

    # Provides various JDBC specific functionality required by Rubyrep.
    module JdbcSQLExtender
      RR::ConnectionExtenders.register :jdbc => self
      
      # Monkey patch for activerecord-jdbc-adapter-0.7.2 as it doesn't set the 
      # +@active+ flag to false, thus ActiveRecord#active? incorrectly confirms
      # the connection to still be active.
      def disconnect!
        super
        @active = false
      end

      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        if not tables.include? table
          raise "table '#{table}' does not exist"
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

      # Returns for each given table, which other tables it references via
      # foreign key constraints.
      # * tables: an array of table names
      # * returns: a hash with
      #   * key: name of the referencing table
      #   * value: an array of names of referenced tables
      def referenced_tables(tables)
        result = {}
        tables.each do |table|
          references_of_this_table = []
          result_set = @connection.connection.getMetaData.getImportedKeys(nil, nil, table)
          while result_set.next
            referenced_table = result_set.getString("PKTABLE_NAME")
            unless references_of_this_table.include? referenced_table
              references_of_this_table << referenced_table
            end
          end
          result[table] = references_of_this_table
        end
        result
      end
    end

    # PostgreSQL specific functionality not provided by the standard JDBC
    # connection extender:
    # * Hack to get schema support for Postgres under JRuby on par with the
    #   standard ruby version.
    module JdbcPostgreSQLExtender

      # Returns the list of a table's column names, data types, and default values.
      #
      # The underlying query is roughly:
      #  SELECT column.name, column.type, default.value
      #    FROM column LEFT JOIN default
      #      ON column.table_id = default.table_id
      #     AND column.num = default.column_num
      #   WHERE column.table_id = get_table_id('table_name')
      #     AND column.num > 0
      #     AND NOT column.is_dropped
      #   ORDER BY column.num
      #
      # If the table name is not prefixed with a schema, the database will
      # take the first match from the schema search path.
      #
      # Query implementation notes:
      #  - format_type includes the column size constraint, e.g. varchar(50)
      #  - ::regclass is a function that gives the id for a table name
      def column_definitions(table_name) #:nodoc:
        rows = select_all(<<-end_sql)
            SELECT a.attname as name, format_type(a.atttypid, a.atttypmod) as type, d.adsrc as default, a.attnotnull as notnull
              FROM pg_attribute a LEFT JOIN pg_attrdef d
                ON a.attrelid = d.adrelid AND a.attnum = d.adnum
             WHERE a.attrelid = '#{table_name}'::regclass
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum
        end_sql
          
        rows.map do |row|
          [row['name'], row['type'], row['default'], row['notnull']]
        end
      end

      require 'jdbc_adapter/jdbc_postgre'
      class JdbcPostgreSQLColumn < ActiveRecord::ConnectionAdapters::Column
        include ::JdbcSpec::PostgreSQL::Column
      end

      # Returns the list of all column definitions for a table.
      def columns(table_name, name = nil)
        # Limit, precision, and scale are all handled by the superclass.
        column_definitions(table_name).collect do |name, type, default, notnull|
          JdbcPostgreSQLColumn.new(name, default, type, notnull == 'f')
        end
      end

      # Sets the schema search path as per configuration parameters
      def initialize_search_path
        execute "SET search_path TO #{config[:schema_search_path]}" if config[:schema_search_path]
      end

      # Returns the active schema search path.
      def schema_search_path
        @schema_search_path ||= select_one('SHOW search_path')['search_path']
      end

      # Returns the list of all tables in the schema search path or a specified schema.
      def tables(name = nil)
        schemas = schema_search_path.split(/,/).map { |p| quote(p) }.join(',')
        select_all(<<-SQL, name).map { |row| row['tablename'] }
          SELECT tablename
            FROM pg_tables
           WHERE schemaname IN (#{schemas})
        SQL
      end

      # Converts the given Time object into the correctly formatted string
      # representation.
      # 
      # Monkeypatched as activerecord-jdbcpostgresql-adapter (at least in version
      # 0.8.2) does otherwise "loose" the microseconds when writing Time values
      # to the database.
      def quoted_date(value)
        "#{value.strftime("%Y-%m-%d %H:%M:%S")}#{value.respond_to?(:usec) ? ".#{value.usec.to_s.rjust(6, '0')}" : ""}"
      end
    end
  end
end

