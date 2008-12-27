require 'rake'
require 'benchmark'

require File.dirname(__FILE__) + '/../sim_helper'

class LeftBigScan < ActiveRecord::Base
  set_table_name "big_scan"
  include CreateWithKey
end

class RightBigScan < ActiveRecord::Base
  set_table_name "big_scan"
  include CreateWithKey
end

# Prepares the database schema for the performance tests.
# * +config+: hash of datbase connection parameters
def prepare_schema(config)
  ActiveRecord::Base.establish_connection config

  ActiveRecord::Schema.define do
    drop_table :big_scan rescue nil
    create_table :big_scan do |t|
        t.column :diff_type, :string
        t.string :text1, :text2, :text3, :text4
        t.text :text5
        t.binary :text6
        t.integer :number1, :number2, :number3
        t.float :number4
      end rescue nil
    end
  end  

BIG_SCAN_RECORD_NUMBER = 5000 # number of records to create for simulation
BIG_SCAN_SEED = 123 # random number seed to make simulation repeatable

# Percentage values for same, modified, left_only and right_only records in simulation
BIG_SCAN_SAME = 95
BIG_SCAN_MODIFIED = BIG_SCAN_SAME + 3
BIG_SCAN_LEFT_ONLY = BIG_SCAN_MODIFIED + 1 # difference to 100% will be right_only records 

def text_columns
  @@text_columns ||= LeftBigScan.column_names.select {|column_name| column_name =~ /^text/}
end

def number_columns
  @@number_columns ||= LeftBigScan.column_names.select {|column_name| column_name =~ /^number/}
end

def random_attributes
  attributes = {}
  text_columns.each {|column_name| attributes[column_name] = "text#{rand(1000)}"}
  number_columns.each {|column_name| attributes[column_name] = rand(1000)}
  attributes
end

# Populates the big_scan tables with sample data.
def populate_data()
  LeftBigScan.establish_connection RR::Initializer.configuration.left
  RightBigScan.establish_connection RR::Initializer.configuration.right
  
  LeftBigScan.delete_all
  RightBigScan.delete_all
  
  srand BIG_SCAN_SEED

  puts "Populating #{BIG_SCAN_RECORD_NUMBER} records"
  progress_bar = ProgressBar.new BIG_SCAN_RECORD_NUMBER
  
  (1..BIG_SCAN_RECORD_NUMBER).each do |i|
    
    # Updating progress bar
    progress_bar.step
    
    attributes = random_attributes
    attributes['id'] = i
    
    case rand(100)
    when 0...BIG_SCAN_SAME
      attributes['diff_type'] = 'same'
      LeftBigScan.create_with_key attributes
      RightBigScan.create_with_key attributes
    when BIG_SCAN_SAME...BIG_SCAN_MODIFIED
      attributes['diff_type'] = 'conflict'
      LeftBigScan.create_with_key attributes
      
      attribute_name = text_columns[rand(text_columns.size)]
      attributes[attribute_name] = attributes[attribute_name] + 'modified'
      attribute_name = number_columns[rand(number_columns.size)]
      attributes[attribute_name] = attributes[attribute_name] + 10000
      RightBigScan.create_with_key attributes
    when BIG_SCAN_MODIFIED...BIG_SCAN_LEFT_ONLY
      attributes['diff_type'] = 'left'
      LeftBigScan.create_with_key attributes
    else
      attributes['diff_type'] = 'right'
      RightBigScan.create_with_key attributes
    end
  end
end

# Prepares the database for the performance simulations
def prepare
  [:left, :right].each {|arm| prepare_schema(RR::Initializer.configuration.send(arm))}
  puts "time required: " + Benchmark.measure {populate_data}.to_s
end

namespace :sims do
  namespace :performance do
    desc "Prepare database"
    task :prepare do
      prepare
    end
    
    desc "Runs the big_scan simulation"
    task :scan do
      Spec::Runner::CommandLine.run(
        Spec::Runner::OptionParser.parse(
          ['--options', "spec/spec.opts", "./sims/performance/big_scan_spec.rb"],
          $stdout, $stderr))
    end
    
    desc "Runs the big_sync simulation"
    task :sync do
      Spec::Runner::CommandLine.run(
        Spec::Runner::OptionParser.parse(
          ['--options', "spec/spec.opts", "./sims/performance/big_sync_spec.rb"],
          $stdout, $stderr))
    end

    begin
      require 'ruby-prof/task'
      RubyProf::ProfileTask.new do |t|
        t.test_files = FileList["./sims/performance/*_spec.rb"]
        t.output_dir = 'profile'
        t.printer = :flat
        t.min_percent = 1
      end
    rescue LoadError
    end
  end
end