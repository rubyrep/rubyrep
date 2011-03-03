require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ConnectionExtenders do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "db_connect should install the already created logger" do
    configuration = deep_copy(Initializer.configuration)
    io = StringIO.new
    logger = ActiveSupport::BufferedLogger.new(io)
    configuration.left[:logger] = logger
    session = Session.new configuration

    session.left.connection.instance_eval {@logger}.should == logger
    session.right.connection.instance_eval {@logger}.should_not == logger

    session.left.select_one "select 'left_query'"
    session.right.select_one "select 'right_query'"

    io.string.should =~ /left_query/
    io.string.should_not =~ /right_query/
  end

  it "db_connect should create and install the specified logger" do
    configuration = deep_copy(Initializer.configuration)
    io = StringIO.new
    configuration.left[:logger] = io
    session = Session.new configuration
    session.left.select_one "select 'left_query'"
    session.right.select_one "select 'right_query'"

    io.string.should =~ /left_query/
    io.string.should_not =~ /right_query/
  end
end

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
  
  it "initialize should establish the database connections" do
    mock_active_record :once
    
    ConnectionExtenders.db_connect Initializer.configuration.left
  end

  it "db_connect should use jdbc configuration adapter and extender under jruby" do
    fake_ruby_platform 'java' do
      mock_active_record :once
      used_extender = nil
      ConnectionExtenders.extenders.should_receive('[]'.to_sym).once \
        .and_return {|extender| used_extender = extender }

      configuration = deep_copy(Initializer.configuration)
      configuration.left[:adapter] = 'dummyadapter'
      
      ConnectionExtenders.db_connect configuration.left
      
      $used_config[:adapter].should == "jdbcdummyadapter"
      used_extender.should == :jdbc
    end
  end

  it "db_connect created connections should be alive" do
    connection = ConnectionExtenders.db_connect Initializer.configuration.left
    
    connection.should be_active
  end
  
  it "db_connect should include the connection extender into connection" do
    connection = ConnectionExtenders.db_connect Initializer.configuration.left

    connection.should respond_to(:primary_key_names)
  end
  
  it "db_connect should raise an Exception if no fitting connection extender is available" do
    # If unknown connection adapters are encountered in jruby, then we
    # automatically use JdbcExtender.
    # Means that this test only makes sense if not running on jruby
    if not RUBY_PLATFORM =~ /java/
      mock_active_record :once

      config = deep_copy(Initializer.configuration)

      config.left[:adapter] = 'dummy'

      lambda {ConnectionExtenders.db_connect  config.left}.should raise_error(RuntimeError, /dummy/)
    end
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

