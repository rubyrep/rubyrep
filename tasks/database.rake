$LOAD_PATH.unshift File.dirname(__FILE__) + "../lib/rubyrep"
require 'rake'
require 'rubyrep'

require File.dirname(__FILE__) + '/task_helper.rb'
require File.dirname(__FILE__) + '/../config/test_config'
require File.dirname(__FILE__) + '/../spec/spec_helper'

# Creates the databases for the given configuration hash
def create_database(config)
  begin
    RR::ConnectionExtenders.db_connect(config)
  rescue
    case config[:adapter]
    when 'postgresql'
      `createdb "#{config[:database]}" -E utf8`
    when 'mysql'
      @charset   = ENV['CHARSET']   || 'utf8'
      @collation = ENV['COLLATION'] || 'utf8_general_ci'
      begin
        connection = RR::ConnectionExtenders.db_connect(config.merge({:database => nil}))
        connection.create_database(config[:database], {:charset => @charset, :collation => @collation})
        RR::ConnectionExtenders.db_connect(config)
      rescue
        $stderr.puts $!
        $stderr.puts "Couldn't create database for #{config.inspect}"
      end
    else
      puts "adapter #{config[:adapter]} not supported"
    end
  else
    puts "database #{config[:database]} already exists"
  end
end

# Drops the databases identified by the given configuration hash
def drop_database(config)
  case config[:adapter]
  when 'postgresql'
    `dropdb "#{config[:database]}"`
  when 'mysql'
    connection = RR::ConnectionExtenders.db_connect(config.merge({'database' => nil}))
    connection.drop_database config[:database]
  else
    puts "adapter #{config[:adapter]} not supported"
  end
end

# Creates the schema and tables for the postgres schema test
def create_postgres_schema(config)
  return unless config[:adapter] == 'postgresql'
  ActiveRecord::Base.establish_connection config
  ActiveRecord::Schema.define do
    execute <<-end_sql
      create schema rr;

      set search_path to rr;
    end_sql

    create_table :rr_simple do |t|
      t.column :name, :string
    end
    execute "insert into rr_simple(id, name) values(1, 'bla')"

    create_table :rr_referenced, :id => true do |t|
      t.column :name, :string
    end

    create_table :rr_referencing, :id => true do |t|
      t.column :rr_referenced_id, :integer
    end

    ActiveRecord::Base.connection.execute(<<-end_sql)
      ALTER TABLE rr_referencing ADD CONSTRAINT rr_referenced_fkey
        FOREIGN KEY (rr_referenced_id)
        REFERENCES rr_referenced(id)
    end_sql

    create_table :rr_trigger_test, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
      t.column :name, :string
    end

    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE rr_trigger_test ADD CONSTRAINT rr_trigger_test_pkey
        PRIMARY KEY (first_id, second_id)
    end_sql

    create_table :rr_sequence_test do |t|
      t.column :name, :string
    end
    
    create_table :rr_duplicate do |t|
      t.column :name, :string
    end

    create_table :rx_pending_changes do |t|
      t.column :change_table, :string
      t.column :change_key, :string
      t.column :change_new_key, :string
      t.column :change_type, :string
      t.column :change_time, :timestamp
    end
  end
end

# Drops the schema and tables that were created for the postgres schema test
def drop_postgres_schema(config)
  return unless config[:adapter] == 'postgresql'
  ActiveRecord::Base.establish_connection config
  ActiveRecord::Schema.define do
    execute "drop schema rr cascade" rescue nil
  end
end

# Creates the sample schema in the database specified by the given 
# configuration.
# * :+database+: either :+left+ or +:right+
# * :+config+: the Configuration object
def create_sample_schema(database, config)
  create_postgres_schema config.send(database)

  ActiveRecord::Base.establish_connection config.send(database)
  
  ActiveRecord::Schema.define do
    create_table :scanner_text_key, :id => false do |t|
      t.column :text_id, :string
      t.column :name, :string
    end rescue nil
    
    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE scanner_text_key ADD CONSTRAINT scanner_text_key_pkey 
        PRIMARY KEY (text_id)
    end_sql

    create_table :scanner_records do |t|
      t.column :name, :string, :null => false
    end rescue nil

    add_index :scanner_records, :name, :unique rescue nil

    create_table :scanner_left_records_only do |t|
      t.column :name, :string, :null => false
    end rescue nil

    create_table :extender_combined_key, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
      t.column :name, :string
    end rescue nil 
    
    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE extender_combined_key ADD CONSTRAINT extender_combined_key_pkey 
        PRIMARY KEY (first_id, second_id)
    end_sql

    create_table :referenced_table, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
      t.column :name, :string
    end

    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE referenced_table ADD CONSTRAINT referenced_table_pkey
        PRIMARY KEY (first_id, second_id)
    end_sql

    create_table :referenced_table2, :id => true do |t|
      t.column :name, :string
    end

    create_table :referencing_table, :id => true do |t|
      t.column :first_fk, :integer
      t.column :second_fk, :integer
      t.column :third_fk, :integer
    end

    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE referencing_table ADD CONSTRAINT referencing_table_fkey
        FOREIGN KEY (first_fk, second_fk)
        REFERENCES referenced_table(first_id, second_id)
    end_sql

    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE referencing_table ADD CONSTRAINT referencing_table2_fkey
        FOREIGN KEY (third_fk)
        REFERENCES referenced_table2(id)
    end_sql

    create_table :extender_inverted_combined_key, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
    end rescue nil 
    
    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE extender_inverted_combined_key 
        ADD CONSTRAINT extender_inverted_combined_key_pkey 
        PRIMARY KEY (second_id, first_id)
    end_sql

    create_table :extender_without_key, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
    end rescue nil 
    
    create_table :extender_one_record do |t|
      t.column :name, :string
    end rescue nil
    
    create_table :extender_no_record do |t|
      t.column :name, :string
    end rescue nil
    
    create_table :extender_type_check do |t|
      t.column :decimal_test, :decimal, :precision => 10, :scale => 5
      t.column :timestamp, :timestamp
      t.column :multi_byte, :string
      t.column :binary_test, :binary
      t.column :text_test, :text
    end rescue nil

    create_table :table_with_manual_key, :id => false do |t|
      t.column :id, :integer
      t.column :name, :string
    end

    create_table :rr_pending_changes do |t|
      t.column :change_table, :string
      t.column :change_key, :string
      t.column :change_new_key, :string
      t.column :change_type, :string
      t.column :change_time, :timestamp
    end

    create_table :rr_logged_events do |t|
      t.column :activity, :string
      t.column :change_table, :string
      t.column :diff_type, :string
      t.column :change_key, :string
      t.column :left_change_type, :string
      t.column :right_change_type, :string
      t.column :description, :string
      t.column :long_description, :string, :limit => RR::ReplicationInitializer::LONG_DESCRIPTION_SIZE
      t.column :event_time, :timestamp
      t.column :diff_dump, :string, :limit => RR::ReplicationInitializer::DIFF_DUMP_SIZE
    end

    create_table :rr_running_flags, :id => false do |t|
      t.column :active, :integer
    end

    create_table :trigger_test, :id => false do |t|
      t.column :first_id, :integer
      t.column :second_id, :integer
      t.column :name, :string
    end

    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE trigger_test ADD CONSTRAINT trigger_test_pkey
        PRIMARY KEY (first_id, second_id)
    end_sql

    create_table :sequence_test do |t|
      t.column :name, :string
    end

    connection = ActiveRecord::Base.connection
    # Neccessary to create tables with dots in ActiveRecord 2.3.5
    def connection.extract_pg_identifier_from_name(name)
      return name, nil
    end

    create_table :table_with_strange_key, :id => false do |t|
      t.column STRANGE_COLUMN, :integer
    end

    ActiveRecord::Base.connection.execute(<<-end_sql)
      ALTER TABLE table_with_strange_key ADD CONSTRAINT table_with_strange_key_pkey
        PRIMARY KEY (#{ActiveRecord::Base.connection.quote_column_name(STRANGE_COLUMN)})
    end_sql

    create_table STRANGE_TABLE do |t|
      t.column :first_fk, :integer
      t.column :second_fk, :integer
      t.column STRANGE_COLUMN, :string
    end

    ActiveRecord::Base.connection.execute(<<-end_sql) 
      ALTER TABLE #{ActiveRecord::Base.connection.quote_table_name(STRANGE_TABLE)} ADD CONSTRAINT strange_table_fkey
        FOREIGN KEY (first_fk, second_fk)
        REFERENCES referenced_table(first_id, second_id)
    end_sql

    create_table :left_table do |t|
      t.column :name, :string
    end if database == :left

    create_table :right_table do |t|
      t.column :name, :string
    end if database == :right

    if config.send(database)[:adapter] == 'postgresql'
      create_table :rr_duplicate, :id => false do |t|
        t.column :blub, :string
      end rescue nil

      ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
        ALTER TABLE rr_duplicate ADD COLUMN key SERIAL
      end_sql

      ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE rr_duplicate ADD CONSTRAINT rr_duplicate_pkey
        PRIMARY KEY (key)
      end_sql

      # duplicate that should *not* be found during PostgreSQL schema support tests
      create_table :rr_referencing do |t|
        t.column :first_fk, :integer
        t.column :second_fk, :integer
      end rescue nil

      ActiveRecord::Base.connection.execute(<<-end_sql)
      ALTER TABLE rr_referencing ADD CONSTRAINT rr_referencing_fkey
        FOREIGN KEY (first_fk, second_fk)
        REFERENCES referenced_table(first_id, second_id)
      end_sql
    end
  end
end

# Removes all tables from the sample scheme
# config: Hash of configuration values for the desired database connection
def drop_sample_schema(config)
  drop_postgres_schema config

  ActiveRecord::Base.establish_connection config
  
  ActiveRecord::Schema.define do
    drop_table :rr_referencing rescue nil
    drop_table :rr_duplicate rescue nil
    drop_table :table_with_strange_key rescue nil
    drop_table STRANGE_TABLE rescue nil
    drop_table :extender_type_check rescue nil
    drop_table :extender_no_record rescue nil
    drop_table :extender_one_record rescue nil
    drop_table :extender_without_key rescue nil
    drop_table :extender_inverted_combined_key rescue nil
    drop_table :extender_combined_key rescue nil
    drop_table :scanner_left_records_only rescue nil
    drop_table :scanner_records rescue nil
    drop_table :scanner_text_key rescue nil
    drop_table :referencing_table rescue nil
    drop_table :referenced_table rescue nil
    drop_table :referenced_table2 rescue nil
    drop_table :table_with_manual_key
    drop_table :rr_pending_changes rescue nil
    drop_table :rr_logged_events rescue nil
    drop_table :rr_running_flags rescue nil
    drop_table :trigger_test rescue nil
    drop_table :sequence_test rescue nil
    drop_table :left_table rescue nil
    drop_table :right_table rescue nil
  end  

  ActiveRecord::Base.connection.disconnect!
end

class ExtenderCombinedKey < ActiveRecord::Base
  set_table_name "extender_combined_key"
  include CreateWithKey  
end

class ScannerRecords < ActiveRecord::Base
  include CreateWithKey
end

class ScannerLeftRecordsOnly < ActiveRecord::Base
  set_table_name "scanner_left_records_only"
  include CreateWithKey
end

class ExtenderOneRecord < ActiveRecord::Base
  set_table_name "extender_one_record"
  include CreateWithKey
end

class ExtenderTypeCheck < ActiveRecord::Base
  set_table_name "extender_type_check"
  include CreateWithKey
end

# Inserts the row as per specified :column_name => value hash in the specified database and table
# Used to create data in tables where the standard ActiveRecord approach doesn't work
# (E. g. tables with primary keys in text format)
def create_row(connection, table, row)
  sql = "insert into #{table}("
  sql << row.keys.join(', ')
  sql << ") values('"
  sql << row.values.join("', '")
  sql << "')"
  connection.execute sql
end

# Deletes all records and creates the records being same in left and right DB
def delete_all_and_create_shared_sample_data(config)
  ActiveRecord::Base.establish_connection config
  ScannerRecords.delete_all
  ScannerRecords.create_with_key :id => 1, :name => 'Alice - exists in both databases'
  
  ExtenderOneRecord.delete_all
  ExtenderOneRecord.create_with_key :id => 1, :name => 'Alice'
  
  ExtenderTypeCheck.delete_all
  ExtenderTypeCheck.create_with_key(
    :id => 1, 
    :decimal_test => 1.234,
    :timestamp => Time.local(2007,"nov",10,20,15,1),
    :multi_byte => "よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.",
    :binary_test => Marshal.dump(['bla',:dummy,1,2,3])
  )

  connection = ActiveRecord::Base.connection
  # The primary key of this table is in text format - ActiveRecord cannot be
  # used to create the example data.
  connection.execute "delete from scanner_text_key"
  [ {:text_id => 'a', :name => 'Alice'},
    {:text_id => 'b', :name => 'Bob'},
    {:text_id => 'c', :name => 'Charlie'}
  ].each { |row| create_row connection, 'scanner_text_key', row}

  # ActiveRecord also doesn't handle tables with combined primary keys
  connection.execute("delete from extender_combined_key")
  [
    {:first_id => 1, :second_id => 1, :name => 'aa'},
    {:first_id => 1, :second_id => 2, :name => 'ab'},
    {:first_id => 2, :second_id => 1, :name => 'ba'},
    {:first_id => 3, :second_id => 1}
  ].each { |row| create_row connection, 'extender_combined_key', row}

  connection.execute("delete from referenced_table")
  connection.execute("delete from referencing_table")
  create_row connection, 'referenced_table', {
    :first_id => 1, :second_id => 2, :name => 'bla'
  }
  create_row connection, 'referencing_table', {:first_fk => 1, :second_fk => 2}
end

# Reinitializes the sample schema with the sample data
def create_sample_data
  # Create records existing in both databases
  [:left, :right].each do |database|
    delete_all_and_create_shared_sample_data RR::Initializer.configuration.send(database)
  end

  # Create data in left table
  ActiveRecord::Base.establish_connection RR::Initializer.configuration.left
  ScannerRecords.create_with_key :id => 2, :name => 'Bob - left database version'
  ScannerRecords.create_with_key :id => 3, :name => 'Charlie - exists in left database only'
  ScannerRecords.create_with_key :id => 5, :name => 'Eve - exists in left database only'

  ScannerLeftRecordsOnly.delete_all
  ScannerLeftRecordsOnly.create_with_key :id => 1, :name => 'Alice'
  ScannerLeftRecordsOnly.create_with_key :id => 2, :name => 'Bob'
  
  # Create data in right table
  ActiveRecord::Base.establish_connection RR::Initializer.configuration.right
  ScannerRecords.create_with_key :id => 2, :name => 'Bob - right database version'
  ScannerRecords.create_with_key :id => 4, :name => 'Dave - exists in right database only'
  ScannerRecords.create_with_key :id => 6, :name => 'Fred - exists in right database only'
end

namespace :db do
  namespace :test do
    
    desc "Creates the test databases"
    task :create do
      create_database RR::Initializer.configuration.left rescue nil
      create_database RR::Initializer.configuration.right rescue nil
    end
    
    desc "Drops the test databases"
    task :drop do
      drop_database RR::Initializer.configuration.left rescue nil
      drop_database RR::Initializer.configuration.right rescue nil
    end
    
    desc "Rebuilds the test databases & schemas"
    task :rebuild => [:drop_schema, :drop, :create, :create_schema, :populate]
    
    desc "Create the sample schemas"
    task :create_schema do
      create_sample_schema :left, RR::Initializer.configuration
      create_sample_schema :right, RR::Initializer.configuration rescue nil
    end
    
    desc "Writes the sample data"
    task :populate do
      create_sample_data
    end

    desc "Drops the sample schemas"
    task :drop_schema do
      # Since Rails 2.2 ActiveRecord doesn't release the database connection
      # anymore. Thus the dropping of the database fails.
      # Workaround:
      # Execute the schema removal in a sub process. Once the sub process
      # exits, it's database connections die.
      pid = Process.fork
      if pid
        Process.wait pid
      else
        drop_sample_schema RR::Initializer.configuration.left
        drop_sample_schema RR::Initializer.configuration.right
        Kernel.exit!
      end
    end
  end
end