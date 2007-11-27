require File.dirname(__FILE__) + '/spec_helper.rb'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe ProxySession do
  before(:each) do
    Initializer.reset
    load config_file
  end

  it "initialize should connect to the database" do
    session = ProxySession.new Initializer.configuration.left
    session.connection.active?.should == true
  end

  it "destroy should disconnect from the database" do
    session = ProxySession.new Initializer.configuration.left
    session.destroy

    session.connection.active?.should == false
  end
  
  it "primary_key_names should return the primary keys of the given table" do
    session = ProxySession.new Initializer.configuration.left
    
    session.primary_key_names('scanner_records').should == ['id']
  end
end