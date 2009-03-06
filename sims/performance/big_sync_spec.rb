require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'
require File.dirname(__FILE__) + '/../sim_helper'

include RR

describe "Big Sync" do
  before(:each) do
  end

  # Calculates and returns number of records of a given type.
  # * :session: current Session instance
  # * :record_type: type of records (String)
  # * :database: designates database, either :left or :right
  def record_quantity(session, record_type, database)
    session.send(database).select_one( \
        "select count(id) as count from big_scan where diff_type = '#{record_type}'")['count'].to_i
  end

  # Calculates and returns the sum of the number fields of records of the given type.
  # * :session: current Session instance
  # * :record_type: type of records (:left, :right, :same or :conflict)
  # * :database: designates database, either :left or :right
  def record_sum(session, record_type, database)
    session.send(database).select_one( \
        "select sum(number1) + sum(number2) + sum(number3) + sum(number4) as sum
         from big_scan where diff_type = '#{record_type}'")['sum'].to_f
  end

  # Runs a sync of the big_scan table.
  # * session: the current Session
  # * expected_result: a hash of record quantities that should result.
  def run_sync(session, expected_result)
    begin
      number_records =
        record_quantity(session, :left, :left) +
        record_quantity(session, :conflict, :left) +
        record_quantity(session, :same, :left) +
        record_quantity(session, :right, :right)

      number_differences =
        record_quantity(session, :left, :left) +
        record_quantity(session, :conflict, :left) +
        record_quantity(session, :right, :right)

      puts "\nSyncing (#{Initializer.configuration.options[:syncer]}, #{session.proxied? ? :proxied : :direct}) table big_scan (#{number_differences} differences in #{number_records} records)"

      sync = TableSync.new(session, 'big_scan')
      sync.progress_printer = RR::ScanProgressPrinters::ProgressBar
      benchmark = Benchmark.measure { sync.run { |diff_type, row| } }
      puts "\n  time required: #{benchmark}"

      {
        :conflict_on_left => record_quantity(session, :conflict, :left),
        :conflict_on_right => record_quantity(session, :conflict, :right),
        :conflict_sum_on_left => record_sum(session, :conflict, :left),
        :conflict_sum_on_right => record_sum(session, :conflict, :right),
        :left_on_left => record_quantity(session, :left, :left),
        :left_on_right => record_quantity(session, :left, :right),
        :right_on_left => record_quantity(session, :right, :left),
        :right_on_right => record_quantity(session, :right, :right)
      }.should == expected_result
    ensure
      Committers::NeverCommitter.rollback_current_session
    end
  end

  it "Proxied OneWaySync should sync correctly" do
    ensure_proxy
    Initializer.configuration = deep_copy(proxied_config)
    
    Initializer.configuration.options = {
      :committer => :never_commit,
      :syncer => :one_way,
      :direction => :right,
      :delete => true,
      :proxy_block_size => 100,
    }
    
    session = Session.new
    expected_result = {
      :conflict_on_left => record_quantity(session, :conflict, :left),
      :conflict_on_right => record_quantity(session, :conflict, :right),
      :conflict_sum_on_left => record_sum(session, :conflict, :left),
      :conflict_sum_on_right => record_sum(session, :conflict, :left),
      :left_on_left => record_quantity(session, :left, :left),
      :left_on_right => record_quantity(session, :left, :left),
      :right_on_left => 0,
      :right_on_right => 0
    }
    run_sync session, expected_result
  end

  it "Direct OneWaySync should sync correctly" do
    Initializer.configuration = deep_copy(standard_config)
    Initializer.configuration.options = {
      :committer => :never_commit,
      :syncer => :one_way,
      :direction => :right,
      :delete => true
    }

    session = Session.new
    expected_result = {
      :conflict_on_left => record_quantity(session, :conflict, :left),
      :conflict_on_right => record_quantity(session, :conflict, :right),
      :conflict_sum_on_left => record_sum(session, :conflict, :left),
      :conflict_sum_on_right => record_sum(session, :conflict, :left),
      :left_on_left => record_quantity(session, :left, :left),
      :left_on_right => record_quantity(session, :left, :left),
      :right_on_left => 0,
      :right_on_right => 0
    }
    run_sync session, expected_result
  end

  it "Proxied TwoWaySync should sync correctly" do
    ensure_proxy
    Initializer.configuration = deep_copy(proxied_config)
    Initializer.configuration.options = {
      :committer => :never_commit,
      :syncer => :two_way,
      :sync_conflict_handling => :right_wins,
      :proxy_block_size => 100,
    }

    session = Session.new
    expected_result = {
      :conflict_on_left => record_quantity(session, :conflict, :left),
      :conflict_on_right => record_quantity(session, :conflict, :right),
      :conflict_sum_on_left => record_sum(session, :conflict, :right),
      :conflict_sum_on_right => record_sum(session, :conflict, :right),
      :left_on_left => record_quantity(session, :left, :left),
      :left_on_right => record_quantity(session, :left, :left),
      :right_on_left => record_quantity(session, :right, :right),
      :right_on_right => record_quantity(session, :right, :right)
    }
    run_sync session, expected_result
  end
end