$LOAD_PATH.unshift File.dirname(__FILE__) + "../lib/rubyrep"
require 'rake'
require 'rubyrep'

require File.dirname(__FILE__) + '/task_helper.rb'
require File.dirname(__FILE__) + '/../config/test_config'

# Creates the databases in the given Configuration object
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
        connection = RR::ConnectionExtenders.db_connect(config.merge({'database' => nil}))
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

# Drops the databases in the given Configuration object
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

# Creates the sample schema in the database specified by the given 
# Configuration object
def create_sample_schema(config)
  connection = RR::ConnectionExtenders.db_connect(config)
  ActiveRecord::Base.connection = connection
  
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

    create_table :rr_change_log do |t|
      t.column :change_table, :string
      t.column :change_key, :string
      t.column :change_org_key, :string
      t.column :change_type, :string
      t.column :change_time, :timestamp
    end

    create_table :rr_active, :id => false do |t|
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
  end
end

# Removes all tables from the sample scheme
# config: Hash of configuration values for the desired database connection
def drop_sample_schema(config)
  connection = RR::ConnectionExtenders.db_connect(config)
  ActiveRecord::Base.connection = connection
  
  ActiveRecord::Schema.define do
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
    drop_table :rr_change_log rescue nil
    drop_table :rr_active rescue nil
    drop_table :trigger_test rescue nil
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
def delete_all_and_create_shared_sample_data(connection)
  ScannerRecords.connection = connection
  ScannerRecords.delete_all
  ScannerRecords.create_with_key :id => 1, :name => 'Alice - exists in both databases'
  
  ExtenderOneRecord.connection = connection
  ExtenderOneRecord.delete_all
  ExtenderOneRecord.create_with_key :id => 1, :name => 'Alice'
  
  ExtenderTypeCheck.connection = connection
  ExtenderTypeCheck.delete_all
  ExtenderTypeCheck.create_with_key(
    :id => 1, 
    :decimal_test => 1.234,
    :timestamp => Time.local(2007,"nov",10,20,15,1),
    :multi_byte => "よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.",
    :binary_test => Marshal.dump(['bla',:dummy,1,2,3])
  )

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
end

# Reinitializes the sample schema with the sample data
def create_sample_data
  session = RR::Session.new
  
  # Create records existing in both databases
  [session.left.connection, session.right.connection].each do |connection|
    delete_all_and_create_shared_sample_data connection
  end

  # Create data in left table
  ScannerRecords.connection = session.left.connection
  ScannerRecords.create_with_key :id => 2, :name => 'Bob - left database version'
  ScannerRecords.create_with_key :id => 3, :name => 'Charlie - exists in left database only'
  ScannerRecords.create_with_key :id => 5, :name => 'Eve - exists in left database only'
  
  # Create data in right table
  ScannerRecords.connection = session.right.connection
  ScannerRecords.create_with_key :id => 2, :name => 'Bob - right database version'
  ScannerRecords.create_with_key :id => 4, :name => 'Dave - exists in right database only'
  ScannerRecords.create_with_key :id => 6, :name => 'Fred - exists in right database only'

  ScannerLeftRecordsOnly.connection = session.left.connection
  ScannerLeftRecordsOnly.delete_all
  ScannerLeftRecordsOnly.create_with_key :id => 1, :name => 'Alice'
  ScannerLeftRecordsOnly.create_with_key :id => 2, :name => 'Bob'
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
      create_sample_schema RR::Initializer.configuration.left rescue nil
      create_sample_schema RR::Initializer.configuration.right rescue nil
    end
    
    desc "Writes the sample data"
    task :populate do
      create_sample_data
    end

    desc "Drops the sample schemas"
    task :drop_schema do
      drop_sample_schema RR::Initializer.configuration.left rescue nil
      drop_sample_schema RR::Initializer.configuration.right rescue nil
    end
  end
end