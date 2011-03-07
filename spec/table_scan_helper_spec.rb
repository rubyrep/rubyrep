require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableScanHelper do
  before(:each) do
    @scan = Object.new
    @scan.extend TableScanHelper
  end

  it "rank_rows should calculate the correct rank of rows based on their primary keys" do
    @scan.stub!(:primary_key_names).and_return(['first_id', 'second_id'])
    @scan.rank_rows({'first_id' => 1, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 1}).should == 0
    @scan.rank_rows({'first_id' => 1, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 2}).should == -1
    @scan.rank_rows({'first_id' => 2, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 1}).should == 1

    # should rank strings according to database logic ('a' < 'A')
    # instead of the Ruby logic (which is the other way round)
    @scan.rank_rows({'first_id' => 'a', 'second_id' => 1}, {'first_id' => 'B', 'second_id' => 1}).should == -1
    @scan.rank_rows({'first_id' => 'a', 'second_id' => 1}, {'first_id' => 'A', 'second_id' => 1}).should == -1
    @scan.rank_rows({'first_id' => 'a', 'second_id' => 1}, {'first_id' => 'a', 'second_id' => 1}).should == 0

    lambda {@scan.rank_rows(nil,nil)}.should raise_error(RuntimeError, 'At least one of left_row and right_row must not be nil!')
    @scan.rank_rows(nil, {'first_id' => 1, 'second_id' => 1}).should == 1
    @scan.rank_rows({'first_id' => 1, 'second_id' => 1}, nil).should == -1
  end

  it "table_scan_class should return TableScan for non-proxied sessions" do
    TableScanHelper.scan_class(Session.new(standard_config)).should == DirectTableScan
  end

  it "table_scan_class should return ProxiedTableScan for proxied sessions" do
    ensure_proxy
    TableScanHelper.scan_class(Session.new(proxied_config)).should == ProxiedTableScan
  end
end
