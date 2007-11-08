$LOAD_PATH.unshift File.dirname(__FILE__) + "../lib/rubyrep"
require 'rake'
require 'rubyrep'
require File.dirname(__FILE__) + '/../config/test_config'

# Creates the databases in the given Configuration object
def create_database(config)
  begin
    ActiveRecord::Base.establish_connection(config)
    ActiveRecord::Base.connection
  rescue
    case config[:adapter]
    when 'postgresql'
      `createdb "#{config[:database]}" -E utf8`
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
  else
    puts "database #{config[:database]} already exists"
  end
end

# Creates the sample schema in the database specified by the give 
# Configuration object
def create_sample_schema(config)
  ActiveRecord::Base.establish_connection(config)
  ActiveRecord::Base.connection
  
  ActiveRecord::Schema.define do
    create_table :authors do |t|
      t.column :name, :string, :null => false
    end rescue nil

    add_index :authors, :name, :unique rescue nil

    create_table :posts do |t|
      t.column :author_id, :integer, :null => false
      t.column :subject, :string
      t.column :body, :text
      t.column :private, :boolean, :default => false
    end rescue nil
    
    create_table :tags do |t|
      t.column :name, :string
    end rescue nil
    
    add_index :tags, :tag_id rescue nil
    
    create_table :posts_tags, :id => false do |t|
      t.column :post_id, :integer
      t.column :tag_id, :integer
    end rescue nil 
    
    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE posts_tags ADD CONSTRAINT posts_tags_pkey 
	PRIMARY KEY (post_id, tag_id)
    end_sql

    create_table :posts_tags_with_inverted_primary_key_index, :id => false do |t|
      t.column :post_id, :integer
      t.column :tag_id, :integer
    end rescue nil 
    
    ActiveRecord::Base.connection.execute(<<-end_sql) rescue nil
      ALTER TABLE posts_tags_with_inverted_primary_key_index 
        ADD CONSTRAINT posts_tags_with_inverted_primary_key_index_pkey 
	PRIMARY KEY (tag_id, post_id)
    end_sql

    create_table :posts_tags_without_primary_key, :id => false do |t|
      t.column :post_id, :integer
      t.column :tag_id, :integer
    end rescue nil 
    
    add_index :posts, :author_id rescue nil
  end
end

# Removes all tables from the sample scheme
# config: Hash of configuration values for the desired database connection
def drop_sample_schema(config)
  ActiveRecord::Base.establish_connection(config)
  ActiveRecord::Base.connection
  
  ActiveRecord::Schema.define do
    drop_table :posts_tags_without_primary_key rescue nil
    drop_table :posts_tags_with_inverted_primary_key_index rescue nil
    drop_table :posts_tags rescue nil
    drop_table :tags rescue nil
    drop_table :authors rescue nil
    drop_table :posts rescue nil
  end  
end

# The standard ActiveRecord#create method ignores primary key attributes.
# This module provides a create method that allows manual setting of primary key values.
module CreateWithKey
  def self.included(base)
    base.extend(ClassMethods)
  end  
  
  module ClassMethods
    # The standard "create" method ignores primary key attributes
    # This method set's _all_ attributes as provided
    def create_with_key attributes
      o = new
      attributes.each do |key, value|
	o[key] = value
      end
      o.save
    end
  end
end

class Authors < ActiveRecord::Base
  include CreateWithKey
end

# Deletes all records and creates the records being same in left and right DB
def delete_all_and_create_shared_sample_data(connection)
  Authors.connection = connection
  Authors.delete_all
  Authors.create_with_key :id => 1, :name => 'Alice - exists in both databases'
end

# Reinitializes the sample schema with the sample data
def create_sample_data
  session = RR::Session.new
  
  # Create records existing in both databases
  [session.left, session.right].each do |connection|
    delete_all_and_create_shared_sample_data connection
  end

  # Create data in left table
  Authors.connection = session.left
  Authors.create_with_key :id => 2, :name => 'Bob - left database version'
  Authors.create_with_key :id => 3, :name => 'Charlie - exists in left database only'
  
  # Create data in right table
  Authors.connection = session.right
  Authors.create_with_key :id => 2, :name => 'Bob - right database version'
  Authors.create_with_key :id => 4, :name => 'Dave - exists in right database only'
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