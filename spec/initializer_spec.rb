require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Initializer do
  before(:each) do
    Initializer::reset
  end

  it "should have an empty configuration" do
    Initializer::configuration.should be_an_instance_of(Configuration)
  end
  
  it "run should yield the configuration object" do
    Initializer::run do |config|
      config.should be_an_instance_of(Configuration)
    end 
  end

  def make_dummy_configuration_change
    Initializer::run do |config|
      config.left = :dummy
    end
  end

  it "configuration should return the current configuration" do
    make_dummy_configuration_change
    Initializer::configuration.should be_an_instance_of(Configuration)
    Initializer::configuration.left.should == :dummy
  end
  
  it "reset should clear the configuration" do
    make_dummy_configuration_change
    Initializer::reset
    Initializer::configuration.left.should be_nil
  end
end

