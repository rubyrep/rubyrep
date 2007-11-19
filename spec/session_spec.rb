require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe Session do
  before(:each) do
    Initializer.reset
    load config_file
    Session.clear_config_cache
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
    
  it "'left=' should store a Connection object and 'left' should return it" do
    mock_active_record
    
    session = Session.new
    
    session.left = :dummy
    session.left.should == :dummy
  end

  it "'right=' should store a Connection object and 'right' should return it" do
    mock_active_record
    
    session = Session.new
    
    session.right = :dummy
    session.right.should == :dummy
  end

  it "initialize shouldn't create the same database connection twice" do
    Left.should_receive(:establish_connection)
    Left.should_receive(:connection)
    Right.should_not_receive(:establish_connection)

    Initializer.configuration.right = Initializer.configuration.left.clone
    
    session = Session.new
  end
  
  it "connections created by initializer should be alive" do
    session = Session.new
    
    session.left.active?.should be_true
    session.left.active?.should be_true
  end
  
  it "initializer should include the connection extender into connection" do
    session = Session.new
    
    session.left.kind_of?(ConnectionExtenders::PostgreSQLExtender).should be_true
  end
  
  it "initializer should raise an Exception if no fitting connection extender is available" do
    Left.should_receive(:establish_connection)
    Left.should_receive(:connection)

    Initializer.configuration.left[:adapter] = :dummy
    
    lambda {session = Session.new}.should raise_error(RuntimeError, /dummy/)
  end
  
  it "initializer should create (fake) proxy connections as per configuration" do
    Initializer::run do |config|
      config.left.merge!({
        :proxy_host => '127.0.0.1',
        :proxy_port => '9876'
      })
    end
    DRbObject.should_receive(:new).with(nil,"druby://127.0.0.1:9876")
    
    session = Session.new

    session.right.should be_an_instance_of(FakeDatabaseProxy)
  end
end

