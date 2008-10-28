require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe Session do # database connection caching is diabled
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
end

describe Session do   # here database connection caching is _not_ disabled
  before(:each) do
    Initializer.configuration = standard_config
  end

  after(:each) do
  end

  it "initialize should create (fake) proxy connections as per configuration" do
    dummy_proxy = Object.new
    dummy_connection = mock("dummy connection")
    dummy_connection.stub!(:tables).and_return([])
    dummy_connection.stub!(:manual_primary_keys=)
    dummy_proxy.should_receive(:create_session).and_return(dummy_connection)
    DRbObject.should_receive(:new).with(nil,"druby://localhost:9876").and_return(dummy_proxy)

    session = Session.new proxied_config

    session.proxies[:left].should == dummy_proxy
    session.proxies[:right].should be_an_instance_of(DatabaseProxy)

    session.left.should == dummy_connection
    session.right.should be_an_instance_of(ProxyConnection)
  end

  it "initialize should assign manual primary keys to the proxy connections" do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables "table_with_manual_key, extender_without_key", :primary_key_names => ['id']
    session = Session.new config
    session.left.manual_primary_keys.should == {'table_with_manual_key'=>['id']}
    session.right.manual_primary_keys.should == {'extender_without_key'=>['id']}
  end

  it "manual_primary_keys should return the correct primary keys" do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables "table_with_manual_key, extender_without_key", :primary_key_names => ['id']
    session = Session.new config
    session.manual_primary_keys(:left).should == {'table_with_manual_key'=>['id']}
    session.manual_primary_keys(:right).should == {'extender_without_key'=>['id']}
  end

  it "configured_table_pairs should return the table pairs as per included_table_specs parameter" do
    session = Session.new
    session.configured_table_pairs(['scanner_records']).should == [
      {:left => 'scanner_records', :right => 'scanner_records'},
    ]
  end

  it "configured_table_pairs should return the table pairs as per configuration if included_table_specs paramter is an empty array" do
    session = Session.new
    session.configured_table_pairs([]).should == [
      {:left => 'scanner_left_records_only', :right => 'scanner_left_records_only'},
      {:left => 'table_with_manual_key', :right => 'table_with_manual_key'}
    ]
  end

  def convert_table_array_to_table_pair_array(tables)
    tables.map {|table| {:left => table, :right => table}}
  end

  it "sort_table_pairs should sort the tables correctly" do
    table_pairs = convert_table_array_to_table_pair_array([
      'scanner_records',
      'referencing_table',
      'referenced_table',
      'scanner_text_key',
    ])
    sorted_table_pairs = convert_table_array_to_table_pair_array([
      'scanner_records',
      'referenced_table',
      'referencing_table',
      'scanner_text_key',
    ])

    Session.new.sort_table_pairs(table_pairs).should == sorted_table_pairs
  end
end

