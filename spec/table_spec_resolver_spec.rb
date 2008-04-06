require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSpecResolver do
  before(:each) do
    Initializer.configuration = standard_config
    @session = Session.new
    @resolver = TableSpecResolver.new @session
  end

  it "initialize should store the session and cache the tables of the session" do
    @resolver.session.should == @session
    @session.should_not_receive :right # ensure that actually the 'left' tables are taken
    @resolver.tables.should == @session.left.tables
  end
  
  it "resolve should resolve direct table names correctly" do
    @resolver.resolve(['bla', 'blub']).should == [
      {:left_table => 'bla', :right_table => 'bla'},
      {:left_table => 'blub', :right_table => 'blub'}
    ]
  end
  
  it "resolve should resolve table name pairs correctly" do
    @resolver.resolve(['my_left_table , my_right_table']).should == [
      {:left_table => 'my_left_table', :right_table => 'my_right_table'}
    ]
  end
  
  it "resolve shold resolve regular expressions correctly" do 
    @resolver.resolve(['/SCANNER_RECORDS|scanner_text_key/']).sort { |a,b|
      a[:left_table] <=> b[:left_table]
    }.should == [
      {:left_table => 'scanner_records', :right_table => 'scanner_records'},
      {:left_table => 'scanner_text_key', :right_table => 'scanner_text_key'}
    ]
  end

end