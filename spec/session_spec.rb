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
  
  it "initialize should keep a reference of the Configuration object" do
    mock_active_record :twice
    
    session = Session.new(Initializer.configuration)
    session.configuration.should == Initializer.configuration
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
  
  it "initializer should create (fake) proxy connections as per configuration" do
    dummy_proxy = Object.new
    dummy_proxy.should_receive(:create_session).and_return(:dummy_proxy_session)
    DRbObject.should_receive(:new).with(nil,"druby://localhost:9876").and_return(dummy_proxy)
    
    session = Session.new proxied_config
    
    session.proxies[:left].should == dummy_proxy
    session.proxies[:right].should be_an_instance_of(DatabaseProxy)
    
    session.left.should == :dummy_proxy_session
    session.right.should be_an_instance_of(ProxyConnection)
  end
end

