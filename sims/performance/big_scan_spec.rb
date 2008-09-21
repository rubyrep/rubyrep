require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'
require File.dirname(__FILE__) + '/../sim_helper'

include RR

describe "Big Scan" do
  before(:each) do
  end

  # Runs a scan of the big_scan table
  def run_scan
    session = Session.new
    expected_result = {}
    expected_result[:conflict] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
    expected_result[:left] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
    expected_result[:right] = session.right.select_one( \
        "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i
    
    number_records = session.left.select_one( \
        "select count(id) as count from big_scan")['count'].to_i \
      + expected_result[:right]
    number_differences = expected_result.values.inject {|sum, n| sum + n }
    received_result = {:conflict => 0, :left => 0, :right => 0}

    table_scan_class = TableScanHelper.scan_class(session)
    puts "\nScanning table big_scan (#{number_differences} differences in #{number_records} records) using #{table_scan_class.name}"

    scan = table_scan_class.new session, 'big_scan'
    scan.progress_printer = RR::ScanProgressPrinters::ProgressBar
    benchmark = Benchmark.measure {
      scan.run do |diff_type, row|
        received_result[diff_type] += 1
      end
    }
    puts "\n  time required: #{benchmark}"
    
    received_result.should == expected_result
  end
  
  it "ProxiedTableScan should identify differences between big_scan tables correctly" do
    Initializer.configuration = deep_copy(proxied_config)
    Initializer.configuration.options[:proxy_block_size] = 100
    ensure_proxy
    
    run_scan
  end

  it "DirectTableScan should identify differences between big_scan tables correctly" do
    Initializer.configuration = standard_config
    
    run_scan
  end
end