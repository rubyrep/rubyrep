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
    ConnectionExtenders::DummyActiveRecord.should_receive(:establish_connection).send(number_of_calls) \
      .and_return {|config| $used_config = config}
    
    dummy_connection = Object.new
    # We have a spec testing behaviour for non-existing extenders.
    # So extend might not be called in all cases
    dummy_connection.should_receive(:extend).any_number_of_times
    
    ConnectionExtenders::DummyActiveRecord.should_receive(:connection).send(number_of_calls) \
      .and_return {dummy_connection}
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
  
  it "initialize should use jdbc configuration adapter and extender under jruby" do
    fake_ruby_platform 'java' do
      mock_active_record :twice
      used_extender = nil
      ConnectionExtenders.extenders.should_receive('[]'.to_sym).twice \
        .and_return {|extender| used_extender = extender }

      Initializer.configuration = deep_copy(Initializer.configuration)
      Initializer.configuration.right[:adapter] = 'dummyadapter'
      
      session = Session.new
      
      $used_config[:adapter].should == "jdbcdummyadapter"
      used_extender.should == :jdbc
    end
  end
  
  it "initialize should not use jdbc configuration adapter and extender under jruby for mysql connections" do
    fake_ruby_platform 'java' do
      mock_active_record :twice
      used_extender = nil
      ConnectionExtenders.extenders.should_receive('[]'.to_sym).twice \
        .and_return {|extender| used_extender = extender }

      Initializer.configuration = deep_copy(Initializer.configuration)
      Initializer.configuration.right[:adapter] = 'mysql'
      
      session = Session.new
      
      $used_config[:adapter].should == "mysql"
      used_extender.should == :mysql
    end
  end
  
  it "connections created by initializer should be alive" do
    session = Session.new
    
    session.left.active?.should be_true
    session.left.active?.should be_true
  end
  
  it "initializer should include the connection extender into connection" do
    session = Session.new
    
    session.left.should respond_to(:select_cursor)
  end
  
  it "initializer should raise an Exception if no fitting connection extender is available" do
    # If unknown connection adapters are encountered in jruby, then we
    # automatically use JdbcExtender.
    # Means that this test only makes sense if not running on jruby
    if not RUBY_PLATFORM =~ /java/
      mock_active_record :once

      config = deep_copy(Initializer.configuration)

      config.left[:adapter] = 'dummy'

      lambda {session = Session.new config}.should raise_error(RuntimeError, /dummy/)
    end
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

