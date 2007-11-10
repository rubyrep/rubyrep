require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe ConnectionExtenders::PostgreSQLExtender do
  before(:each) do
    Initializer.reset
    load File.dirname(__FILE__) + '/../config/test_config.rb'
  end

  it "primary_key_names should return primary key names ordered as per primary key index" do
    session = Session.new
    session.left.primary_key_names('extender_combined_key').should == ['first_id', 'second_id']
    
    session.left.primary_key_names('extender_inverted_combined_key') \
      .should == ['second_id', 'first_id']
  end
  
  it "primary_key_names should return an empty array for tables without any primary key" do
    session = Session.new
    session.left.primary_key_names('extender_without_key') \
      .should == []
  end
  
  it "primary_key_names called for a non-existing table should throw an exception" do
    session = Session.new
    lambda {session.left.primary_key_names('non_existing_table')} \
      .should raise_error(RuntimeError, 'table does not exist')
  end
  
  it "select_cursor should handle zero result queries" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_no_record"
    result.next?.should be_false
  end
  
  it "select_cursor should allow iterating through records" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_one_record"
    result.next?.should be_true
    result.next_row.should == {'id' => "1", 'name' => 'Alice'}
  end
  
  it "select_cursor next_row should raise if there are no records" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_no_record"
    lambda {result.next_row}.should raise_error(RuntimeError, 'no more rows available')  
  end
  
  it "select_cursor next_row should handle uncommon datatypes correctly" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_type_check"
    row = result.next_row
    row['timestamp'] = Time.parse row['timestamp']
    row.should == {
    'id' => "1", 
    'decimal' => BigDecimal.new("1.234"),
    'timestamp' => Time.local(2007,"nov",10,20,15,1),
    'byteea' => "dummy"}
  end
end