require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'
require File.dirname(__FILE__) + '/../sim_helper'

include RR

describe "Big Scan" do
  before(:each) do
  end

  # Runs a scan of the big_scan table using the provided Scanner class
  def run_scan(table_scan_class)
    puts "\nScanning table big_scan using #{table_scan_class.name}"
    session = Session.new
    expected_result = {}
    expected_result[:conflict] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
    expected_result[:left] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
    expected_result[:right] = session.right.select_one( \
        "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i
    
    received_result = {:conflict => 0, :left => 0, :right => 0}
    
    progress_bar = ProgressBar.new expected_result.values.inject {|sum, n| sum + n }

    scan = table_scan_class.new session, 'big_scan'
    benchmark = Benchmark.measure {
      scan.run do |diff_type, row|
        progress_bar.step
        received_result[diff_type] += 1
      end
    }
    puts "  time required: #{benchmark}"
    
    received_result.should == expected_result
  end
  
  it "ProxiedTableScan should identify differences between big_scan tables correctly" do
    Initializer.configuration = deep_copy(proxied_config)
    Initializer.configuration.proxy_options[:block_size] = 100
    ensure_proxy
    
    run_scan ProxiedTableScan
  end

  it "DirectTableScan should identify differences between big_scan tables correctly" do
    Initializer.configuration = standard_config
    
    run_scan DirectTableScan
  end
end