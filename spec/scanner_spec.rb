require File.dirname(__FILE__) + '/spec_helper.rb'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe Scanner do
  before(:each) do
    Initializer.reset
    load config_file
  end

  it "construct_query should create the query for scanning the table" do
    session = Session.new
    scanner = Scanner.new session, 'scanner_records'
    scanner.construct_query.should == 'select id, name from scanner_records order by id'
  end

  it "construct_query should handle combined primary keys correctly" do
    session = Session.new
    scanner = Scanner.new session, 'extender_combined_key'
    scanner.construct_query.should == 'select first_id, second_id from extender_combined_key order by first_id, second_id'
  end

  it "initialize should raise exception if table doesn't have primary keys" do
    session = Session.new
    lambda {Scanner.new session, 'extender_without_key'} \
      .should raise_error(RuntimeError, "Table extender_without_key doesn't have a primary key. Cannot scan.")
  end

end