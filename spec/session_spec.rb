require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

CONFIG_FILE = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe Session do
  before(:each) do
    Initializer.reset
    load CONFIG_FILE
  end

  def mock_active_record
    Left.should_receive(:establish_connection)
    Left.should_receive(:connection)
    Right.should_receive(:establish_connection)
    Right.should_receive(:connection)    
  end
  it "initialize should make a deep copy of the Configuration object" do
    mock_active_record
    
    session = Session.new
    session.configuration.left.should == Initializer.configuration.left
    session.configuration.right.should == Initializer.configuration.right
    
    Initializer.configuration.left[:dummy] = :dummy_value
    session.configuration.left.has_key?(:dummy).should be_false
  end
  
  it "initialize should establish the database connections" do
    mock_active_record
    
    session = Session.new
  end
  
  it "initialize shouldn't create the same database connection twice" do
    Left.should_receive(:establish_connection)
    Left.should_receive(:connection)

    Initializer.configuration.right = Initializer.configuration.left.clone
    
    session = Session.new
  end
  
  it "connections created by initializer should be alive" do
    session = Session.new
    
    session.left.active?.should be_true
    session.left.active?.should be_true
  end
end

