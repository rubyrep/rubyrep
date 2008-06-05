require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe DatabaseProxy do
  before(:each) do
    Initializer.configuration = standard_config
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

    session.should be_an_instance_of(ProxyConnection)
    proxy.session_register.include?(session).should == true
  end

  it "destroy_session should destroy and unregister the session" do
    proxy, session = create_proxy_and_session
    session.should_receive(:destroy)
    
    proxy.destroy_session session

    proxy.session_register.include?(session).should == false
  end
  
  it "ping should respond with 'pong'" do
    proxy = DatabaseProxy.new
    proxy.ping.should == 'pong' 
  end
  
  it "terminate should exit the proxy" do
    proxy = DatabaseProxy.new
    Thread.main.should_receive(:raise).with(SystemExit)
    
    proxy.terminate!
  end
end