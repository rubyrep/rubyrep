require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'
require File.dirname(__FILE__) + '/../sim_helper'

include RR

describe "Big Sync" do
  before(:each) do
  end

  # Runs a sync of the big_scan table
  def run_sync
    begin
      session = Session.new

      expected_result = {}
      expected_result[:conflict] = session.left.select_one( \
          "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
      expected_result[:left] = session.left.select_one( \
          "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
      expected_result[:right] = 0

      number_right_records = session.right.select_one( \
          "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i
      number_records = session.left.select_one( \
          "select count(id) as count from big_scan")['count'].to_i \
        + number_right_records
      number_differences = expected_result.values.inject {|sum, n| sum + n } \
        + number_right_records

      puts "\nSyncing (#{Initializer.configuration.sync_options[:syncer]}, #{session.proxied? ? :proxied : :direct}) table big_scan (#{number_differences} differences in #{number_records} records)"
      progress_bar = ProgressBar.new number_differences

      sync = TableSync.new(session, 'big_scan')
      benchmark = Benchmark.measure {
        sync.run do |diff_type, row|
          progress_bar.step
        end
      }
      puts "  time required: #{benchmark}"

      received_result = {}
      received_result[:conflict] = session.left.select_one( \
          "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
      received_result[:left] = session.left.select_one( \
          "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
      received_result[:right] = session.right.select_one( \
          "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i

      received_result.should == expected_result
    ensure
      Committers::NeverCommitter.rollback_current_session
    end
  end
  
  it "Proxied OneWaySync should sync correctly" do
    ensure_proxy
    Initializer.configuration = deep_copy(proxied_config)
    Initializer.configuration.sync_options = {
      :committer => :never_commit,
      :syncer => :one_way,
      :direction => :right,
      :delete => true
    }
    run_sync
  end

  it "Direct OneWaySync should sync correctly" do
    Initializer.configuration = deep_copy(standard_config)
    Initializer.configuration.sync_options = {
      :committer => :never_commit,
      :syncer => :one_way,
      :direction => :right,
      :delete => true
    }
    run_sync
  end
end