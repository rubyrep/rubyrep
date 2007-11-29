require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Initializer do
  it "should have an empty configuration" do
    Initializer::configuration.should be_an_instance_of(Configuration)
  end
end

describe Initializer do
  before(:each) do
    Initializer::reset
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
  
  it "configuration= should set a new configuration" do
    make_dummy_configuration_change
    Initializer::configuration = :dummy_config
    Initializer::configuration.should == :dummy_config
  end
  
  it "reset should clear the configuration" do
    make_dummy_configuration_change
    Initializer::reset
    Initializer::configuration.left.should {}
  end
end

