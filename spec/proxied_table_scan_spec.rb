require File.dirname(__FILE__) + '/spec_helper.rb'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe ProxiedTableScan do
  before(:each) do
    Initializer.reset
    load config_file
  end

  it "initialize should raise exception if session is not proxied" do
    session = Session.new
    lambda { ProxiedTableScan.new session, 'dummy_table' } \
      .should raise_error(RuntimeError, /only works with proxied sessions/)
  end

  it "initialize should cache the primary keys" do
    proxify!
    ensure_proxy
    session = Session.new
    scan = ProxiedTableScan.new session, 'scanner_records'
    scan.primary_key_names.should == ['id']
  end

  it "initialize should raise exception if table doesn't have primary keys" do
    proxify!
    ensure_proxy
    session = Session.new
    lambda {ProxiedTableScan.new session, 'extender_without_key'} \
      .should raise_error(RuntimeError, "Table extender_without_key doesn't have a primary key. Cannot scan.")
  end

end

