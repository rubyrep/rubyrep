require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe Session do
  before(:each) do
    Initializer.configuration = standard_config
    @@old_cache_status = ConnectionExtenders.use_db_connection_cache(false)
  end
  
  after(:each) do
    ConnectionExtenders.use_db_connection_cache(@@old_cache_status)
  end
  
  # if number_of_calls is :once, mock ActiveRecord for 1 call
  # if number_of_calls is :twice, mock ActiveRecord for 2 calls
  def mock_active_record(number_of_calls)
    ConnectionExtenders::DummyActiveRecord.should_receive(:establish_connection).send number_of_calls
    ConnectionExtenders::DummyActiveRecord.should_receive(:connection).send number_of_calls
  end
  
  it "initialize should make a deep copy of the Configuration object" do
    mock_active_record :twice
    
    session = Session.new
    session.configuration.left.should == Initializer.configuration.left
    session.configuration.right.should == Initializer.configuration.right
    
    session.configuration.left[:adapter].object_id.should_not \
      == Initializer.configuration.left[:adapter].object_id
  end
  
  it "initialize should establish the database connections" do
    mock_active_record :twice
    
    session = Session.new
  end
    
  it "'left=' should store a Connection object and 'left' should return it" do
    mock_active_record :twice
    
    session = Session.new
    
    session.left = :dummy
    session.left.should == :dummy
  end

  it "'right=' should store a Connection object and 'right' should return it" do
    mock_active_record :twice
    
    session = Session.new
    
    session.right = :dummy
    session.right.should == :dummy
  end

  it "initialize shouldn't create the same database connection twice" do
    mock_active_record :once

    Initializer.configuration = deep_copy(Initializer.configuration)
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
    
    # get the ConnectionExtender module for the active database adapter
    extender = ConnectionExtenders.extenders[session.configuration.left[:adapter].to_sym]
    
    session.left.kind_of?(extender).should be_true
  end
  
  it "initializer should raise an Exception if no fitting connection extender is available" do
    mock_active_record :once

    config = deep_copy(Initializer.configuration)
    
    config.left[:adapter] = :dummy
    
    lambda {session = Session.new config}.should raise_error(RuntimeError, /dummy/)
  end
  
  it "initializer should create (fake) proxy connections as per configuration" do
    dummy_proxy = Object.new
    dummy_proxy.should_receive(:create_session).and_return(:dummy_proxy_session)
    DRbObject.should_receive(:new).with(nil,"druby://localhost:9876").and_return(dummy_proxy)
    
    session = Session.new proxied_config
    
    session.proxies[:left].should == dummy_proxy
    session.proxies[:right].should be_an_instance_of(DatabaseProxy)
    
    session.left.should == :dummy_proxy_session
    session.right.should be_an_instance_of(ProxySession)
  end
end

