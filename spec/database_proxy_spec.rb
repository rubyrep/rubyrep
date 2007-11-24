require File.dirname(__FILE__) + '/spec_helper.rb'

config_file = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe DatabaseProxy do
  before(:each) do
    Initializer.reset
    load config_file
  end

  it "initialize should create an empty session regiser" do
    proxy =  DatabaseProxy.new
    proxy.session_register.should == {}
  end

  def create_proxy_and_session
    proxy = DatabaseProxy.new
    session = proxy.create_session Initializer.configuration.left
    return proxy, session
  end

  it "create_session should register the created session" do
    proxy, session = create_proxy_and_session

    session.should be_an_instance_of(ProxySession)
    proxy.session_register.include?(session).should == true
  end

  it "destroy_session should destroy and unregister the session" do
    proxy, session = create_proxy_and_session
    session.should_receive(:destroy)
    
    proxy.destroy_session session

    proxy.session_register.include?(session).should == false
  end
end