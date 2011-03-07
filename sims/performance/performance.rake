
require 'rake'
require 'benchmark'

require File.dirname(__FILE__) + '/../sim_helper'

# Prepares the database schema for the performance tests.
def prepare_schema
  session = RR::Session.new

  [:left, :right].each do |database|
    c = session.send(database)
    [:big_scan, :big_rep, :big_rep_backup].each do |table|
      c.drop_table table rescue nil
      c.create_table table
      c.add_column table, :diff_type, :string
      (1..4).each {|i| c.add_column table, "text#{i}", :string}
      c.add_column table, :text5, :text
      c.add_column table, :text6, :binary
      (1..3).each {|i| c.add_column table, "number#{i}", :integer}
      c.add_column table, :number4, :float
    end
  end
end  

BIG_SCAN_RECORD_NUMBER = 5000 # number of records to create for simulation
BIG_SCAN_SEED = 123 # random number seed to make simulation repeatable

# Percentage values for same, modified, left_only and right_only records in simulation
BIG_SCAN_SAME = 95
BIG_SCAN_MODIFIED = BIG_SCAN_SAME + 3
BIG_SCAN_LEFT_ONLY = BIG_SCAN_MODIFIED + 1 # difference to 100% will be right_only records

def big_scan_columns
  @@big_scan_columns ||= nil
  unless @@big_scan_columns
    session = RR::Session.new
    @@big_scan_columns = session.left.column_names('big_scan')
  end
  @@big_scan_columns
end

def text_columns
  @@text_columns ||= big_scan_columns.select {|column_name| column_name =~ /^text/}
end

def number_columns
  @@number_columns ||= big_scan_columns.select {|column_name| column_name =~ /^number/}
end

def random_attributes
  attributes = {}
  text_columns.each {|column_name| attributes[column_name] = "text#{rand(1000)}"}
  number_columns.each {|column_name| attributes[column_name] = rand(1000)}
  attributes
end

# Populates the big_scan tables with sample data.
def populate_scan_data()
  session = RR::Session.new

  [:left, :right].each {|database| session.send(database).execute "delete from big_scan"}
  
  srand BIG_SCAN_SEED

  puts "\nGenerating #{BIG_SCAN_RECORD_NUMBER} records in big_scan"
  progress_bar = ProgressBar.new BIG_SCAN_RECORD_NUMBER
  
  (1..BIG_SCAN_RECORD_NUMBER).each do |i|
    
    # Updating progress bar
    progress_bar.step
    
    attributes = random_attributes
    attributes['id'] = i
    
    case rand(100)
    when 0...BIG_SCAN_SAME
      attributes['diff_type'] = 'same'
      [:left, :right].each {|database| session.send(database).insert_record 'big_scan', attributes}
    when BIG_SCAN_SAME...BIG_SCAN_MODIFIED
      attributes['diff_type'] = 'conflict'
      session.left.insert_record 'big_scan', attributes
      
      attribute_name = text_columns[rand(text_columns.size)]
      attributes[attribute_name] = attributes[attribute_name] + 'modified'
      attribute_name = number_columns[rand(number_columns.size)]
      attributes[attribute_name] = attributes[attribute_name] + 10000
      session.right.insert_record 'big_scan', attributes
    when BIG_SCAN_MODIFIED...BIG_SCAN_LEFT_ONLY
      attributes['diff_type'] = 'left'
      session.left.insert_record 'big_scan', attributes
    else
      attributes['diff_type'] = 'right'
      session.right.insert_record 'big_scan', attributes
    end
  end
end

BIG_REP_CHANGE_NUMBER = 5000 # number of records to create for simulation
BIG_REP_SEED = 456 # random number seed to make simulation repeatable

# Percentage values for inserts, updates and deletes in simulation
BIG_REP_INSERT = 45
BIG_REP_UPDATE = BIG_REP_INSERT + 30 # different to 100% will be deletes

# Populates the big_rep tables with sample data and changes.
def populate_rep_data
  session = RR::Session.new
  initializer = RR::ReplicationInitializer.new session

  # step 1: clear change log; ensure trigger, initialize big_rep table from data in big_scan
  [:left, :right].each do |database|
    initializer.drop_trigger(database, 'big_rep') rescue nil
    session.send(database).execute "delete from big_rep"
    session.send(database).execute "insert into big_rep select * from big_scan where diff_type = 'same'"
    session.send(database).execute "delete from rr_pending_changes"
    initializer.create_trigger(database, 'big_rep')
  end

  # step 2: generate changes

  srand BIG_REP_SEED

  # Keep tracks of the record ids in each database
  all_ids = {}
  all_ids[:left] = session.left.select_all("select id from big_rep").map {|row| row['id']}
  all_ids[:right] = all_ids[:left].clone

  # Next available id value
  next_id = session.left.select_one("select max(id) + 1 as id from big_rep")['id'].to_i

  puts "\nGenerating #{BIG_REP_CHANGE_NUMBER} changes in big_rep"
  progress_bar = ProgressBar.new BIG_REP_CHANGE_NUMBER
  (1..BIG_REP_CHANGE_NUMBER).each do

    # Updating progress bar
    progress_bar.step
    
    database = [:left, :right][rand(2)]

    case rand(100)
    when 0...BIG_REP_INSERT
      attributes = random_attributes
      attributes['diff_type'] = 'insert'
      attributes['id'] = next_id
      all_ids[database] << next_id
      next_id += 1
      session.send(database).insert_record 'big_rep', attributes
    when BIG_REP_INSERT...BIG_REP_UPDATE
      id = all_ids[database][rand(all_ids[database].size)]
      attributes = session.send(database).select_one("select * from big_rep where id = '#{id}'")
      column = number_columns[rand(number_columns.size)]
      attributes[column] = rand(1000)
      session.send(database).update_record 'big_rep', attributes
    else
      i = rand(all_ids[database].size)
      id = all_ids[database].delete_at(i)
      session.send(database).delete_record 'big_rep', 'id' => id
    end
  end

  # step 3: move data into backup tables
  [:left, :right].each do |database|
    session.send(database).execute "delete from big_rep_backup"
    session.send(database).execute "insert into big_rep_backup select * from big_rep"
    session.send(database).drop_table "big_rep_pending_changes" rescue nil
    session.send(database).execute "create table big_rep_pending_changes as select * from rr_pending_changes"
    initializer.drop_trigger database, 'big_rep'
    session.send(database).execute "delete from big_rep"
    session.send(database).execute "delete from rr_pending_changes"
  end
end

# Generates the sample data
def populate_data
  populate_scan_data
  populate_rep_data
end

# Prepares the database for the performance simulations
def prepare
  prepare_schema
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

    desc "Runs the big_rep simulation"
    task :rep do
      Spec::Runner::CommandLine.run(
        Spec::Runner::OptionParser.parse(
          ['--options', "spec/spec.opts", "./sims/performance/big_rep_spec.rb"],
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