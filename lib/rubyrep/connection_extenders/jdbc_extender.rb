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
        if tables.grep(/^#{table}$/i).empty?
          # Note: Cannot use tables.include? as returned tables are made lowercase under JRuby MySQL
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
      require 'jdbc_adapter/jdbc_postgre'
      require "#{File.dirname(__FILE__)}/postgresql_extender"

      include PostgreSQLExtender

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

