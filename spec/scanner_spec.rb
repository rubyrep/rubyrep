require File.dirname(__FILE__) + '/spec_helper.rb'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe DirectTableScan do
  before(:each) do
    Initializer.reset
    load config_file
  end

  it "construct_query should create the query for scanning the table" do
    session = Session.new
    scanner = DirectTableScan.new session, 'scanner_records'
    scanner.construct_query('scanner_records').should == 'select id, name from scanner_records order by id'
  end

  it "construct_query should handle combined primary keys correctly" do
    session = Session.new
    scanner = DirectTableScan.new session, 'extender_combined_key'
    scanner.construct_query('extender_combined_key').should == 'select first_id, second_id from extender_combined_key order by first_id, second_id'
  end

  it "initialize should raise exception if table doesn't have primary keys" do
    session = Session.new
    lambda {DirectTableScan.new session, 'extender_without_key'} \
      .should raise_error(RuntimeError, "Table extender_without_key doesn't have a primary key. Cannot scan.")
  end

  it "initialize should cache the primary keys of the given table" do
    session = Session.new
    scanner = DirectTableScan.new session, 'scanner_records'
    scanner.primary_key_names.should == ['id']
  end

  it "initialize should use the name of the left table as overwritable default for right table" do
    session = Session.new
    DirectTableScan.new(session, 'scanner_records').right_table.should == 'scanner_records'
    DirectTableScan.new(session, 'scanner_records', 'dummy').right_table.should == 'dummy'
  end

  it "rank_rows should calculate the correct rank of rows based on their primary keys" do
    session = Session.new
    scanner = DirectTableScan.new session, 'extender_combined_key'
    scanner.rank_rows({'first_id' => 1, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 1}).should == 0
    scanner.rank_rows({'first_id' => 1, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 2}).should == -1
    scanner.rank_rows({'first_id' => 2, 'second_id' => 1}, {'first_id' => 1, 'second_id' => 1}).should == 1
  end

  it "run should compare all the records in the table" do
    session = Session.new
    scanner = DirectTableScan.new session, 'scanner_records'
    diff = []
    scanner.run do |type, row|
      diff.push [type, row]
    end
    # in this scenario the right table has the 'highest' data, 
    # so 'right-sided' data are already implicitely tested here
    diff.should == [
      [:conflict, [{'id' => '2', 'name' => 'Bob - left database version'},
          {'id' => '2', 'name' => 'Bob - right database version'}]],
      [:left, {'id' => '3', 'name' => 'Charlie - exists in left database only'}],
      [:right, {'id' => '4', 'name' => 'Dave - exists in right database only'}],
      [:left, {'id' => '5', 'name' => 'Eve - exists in left database only'}],
      [:right, {'id' => '6', 'name' => 'Fred - exists in right database only'}]
    ]
  end

  it "run should handle one-sided data" do
    # separate test case for left-sided data; right-sided data are already covered in the general test
    session = Session.new
    scanner = DirectTableScan.new session, 'scanner_left_records_only'
    diff = []
    scanner.run do |type, row|
      diff.push [type, row]
    end
    diff.should == [
      [:left, {'id' => '1', 'name' => 'Alice'}],
      [:left, {'id' => '2', 'name' => 'Bob'}]
    ]
  end

end