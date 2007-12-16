require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'

include RR

describe "Big Scan" do
  before(:each) do
  end

  it "ProxiedTableScan should identify differences between big_scan tables correctly" do
    puts 'Scanning table big_scan using ProxiedTableScan'
    Initializer.configuration = deep_copy(proxied_config)
    Initializer.configuration.proxy_options[:block_size] = 100
    ensure_proxy
    session = Session.new
    expected_result = {}
    expected_result[:conflict] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
    expected_result[:left] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
    expected_result[:right] = session.right.select_one( \
        "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i
    
    received_result = {:conflict => 0, :left => 0, :right => 0}
    
    scan = ProxiedTableScan.new session, 'big_scan'
    benchmark = Benchmark.measure {
      scan.run { |diff_type, row| received_result[diff_type] += 1 }
    }
    puts "  time required: #{benchmark}"
    
    received_result.should == expected_result
  end

  it "DirectTableScan should identify differences between big_scan tables correctly" do
    puts 'Scanning table big_scan using DirectTableScan'
    Initializer.configuration = standard_config
    session = Session.new
    expected_result = {}
    expected_result[:conflict] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'conflict'")['count'].to_i
    expected_result[:left] = session.left.select_one( \
        "select count(id) as count from big_scan where diff_type = 'left'")['count'].to_i
    expected_result[:right] = session.right.select_one( \
        "select count(id) as count from big_scan where diff_type = 'right'")['count'].to_i
    
    received_result = {:conflict => 0, :left => 0, :right => 0}
    
    scan = DirectTableScan.new session, 'big_scan'
    benchmark = Benchmark.measure {
      scan.run { |diff_type, | received_result[diff_type] += 1 }
    }
    puts "  time required: #{benchmark}"
    
    received_result.should == expected_result
  end
end