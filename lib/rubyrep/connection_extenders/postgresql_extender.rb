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

      # Returns an ordered list of primary key column names of the given table
      def primary_key_names(table)
        row = self.select_one(<<-end_sql)
          SELECT relname
          FROM pg_class
          WHERE relname = '#{table}'
        end_sql
        if row.nil?
        raise "table '#{table}' does not exist"
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

