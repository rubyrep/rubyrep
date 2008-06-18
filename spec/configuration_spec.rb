require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
  end

  it "initialize should set #left and #right to empty hashes" do
    config = Configuration.new
    [:left, :right].each do |hash_attr|
      config.send(hash_attr).should == {}
    end
  end
  
  it "initialize should set #proxy_options to the default proxy options" do
    config = Configuration.new
    config.proxy_options.should == Configuration::DEFAULT_PROXY_OPTIONS
  end

  it "initialize should set #sync_options to the default sync options" do
    config = Configuration.new
    config.sync_options.should == Configuration::DEFAULT_SYNC_OPTIONS
  end
end