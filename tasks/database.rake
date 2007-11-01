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

namespace :db do
  namespace :test do
    
    desc "Creates the test databases"
    task :create do
      create_database RR::Initializer.configuration.left 
      create_database RR::Initializer.configuration.right 
    end
    
    desc "Drops the test databases"
    task :drop do
      drop_database RR::Initializer.configuration.left
      drop_database RR::Initializer.configuration.right
    end
  end
end