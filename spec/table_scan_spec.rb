require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableScan do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should raise exception if table doesn't have primary keys" do
    session = Session.new
    lambda {TableScan.new session, 'extender_without_key'} \
      .should raise_error(RuntimeError, "Table extender_without_key doesn't have a primary key. Cannot scan.")
  end

  it "initialize should cache the primary keys of the given table" do
    session = Session.new
    scann = TableScan.new session, 'scanner_records'
    scann.primary_key_names.should == ['id']
  end

  it "initialize should use the name of the left table as overwritable default for right table" do
    session = Session.new
    TableScan.new(session, 'scanner_records').right_table.should == 'scanner_records'
    TableScan.new(session, 'scanner_records', 'dummy').right_table.should == 'dummy'
  end 
end