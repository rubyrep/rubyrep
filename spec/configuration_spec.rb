require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
  end

  it "initialize should create configuration variables with empty hashes" do
    config = Configuration.new
    [:left, :right, :left_proxy, :right_proxy, :proxy_options].each do |hash_attr|
      config.send(hash_attr).should == {}
    end
    
  end
end