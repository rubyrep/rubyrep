module RR

  module ConnectionExtenders

    # Provides various MySQL specific functionality required by Rubyrep.
    module MysqlExtender
      RR::ConnectionExtenders.register :mysql => self

      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        row = self.select_one(<<-end_sql)
          select table_name from information_schema.tables 
          where table_schema = database() and table_name = '#{table}'
        end_sql
        if row.nil?
          raise "table '#{table}' does not exist"
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

      # Returns for each given table, which other tables it references via
      # foreign key constraints.
      # * tables: an array of table names
      # Returns: a hash with
      # * key: name of the referencing table
      # * value: an array of names of referenced tables
      def referenced_tables(tables)
        rows = self.select_all(<<-end_sql)
          select distinct table_name as referencing_table, referenced_table_name as referenced_table
          from information_schema.key_column_usage
          where table_schema = database()
          and table_name in ('#{tables.join("', '")}')
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
        tables.each do |table|
          result[table] = [] unless result.include? table
        end
        result
      end
    end
  end
end
