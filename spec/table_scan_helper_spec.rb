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
  end

end
