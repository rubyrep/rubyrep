require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ConnectionExtenders, "Registration" do
  before(:each) do
    Initializer.configuration = standard_config
    @@old_cache_status = ConnectionExtenders.use_db_connection_cache(false)
  end

  after(:each) do
    ConnectionExtenders.use_db_connection_cache(@@old_cache_status)
  end
  
  it "extenders should return list of registered connection extenders" do
    ConnectionExtenders.extenders.include?(:postgresql).should be_true
  end
  
  it "register should register a new connection extender" do
    ConnectionExtenders.register(:bla => :blub)
    
    ConnectionExtenders.extenders.include?(:bla).should be_true
  end
  
  it "register should replace already existing connection extenders" do
    ConnectionExtenders.register(:bla => :blub)
    ConnectionExtenders.register(:bla => :blub2)
    
    ConnectionExtenders.extenders[:bla].should == :blub2
  end
  
  it "use_db_connection_cache should set the new cache status and return the old one" do
    ConnectionExtenders.use_db_connection_cache :first_status
    first_status = ConnectionExtenders.use_db_connection_cache :second_status
    second_status = ConnectionExtenders.use_db_connection_cache :whatever
    first_status.should == :first_status
    second_status.should == :second_status
  end
  
  it "clear_db_connection_cache should clear the connection cache" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.connection_cache = :dummy_cache
      ConnectionExtenders.clear_db_connection_cache
      ConnectionExtenders.connection_cache.should == {}
    ensure
      ConnectionExtenders.connection_cache = old_cache
    end
  end
  
  it "db_connect should create the database connection if not yet cached" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.clear_db_connection_cache
      mock_active_record :once
      ConnectionExtenders.use_db_connection_cache true
      ConnectionExtenders.db_connect Initializer.configuration.left
      ConnectionExtenders.connection_cache.should_not be_empty
    ensure
      ConnectionExtenders.connection_cache = old_cache     
    end    
  end
  
  it "db_connect should not create the database connection if already cached and alive" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.clear_db_connection_cache
      mock_active_record :once # only mocked once even though db_connect is called twice
      ConnectionExtenders.use_db_connection_cache true
      connection = ConnectionExtenders.db_connect Initializer.configuration.left
      connection.should_receive(:active?).and_return(:true)
      ConnectionExtenders.db_connect Initializer.configuration.left
    ensure
      ConnectionExtenders.connection_cache = old_cache     
    end    
  end
  
end

