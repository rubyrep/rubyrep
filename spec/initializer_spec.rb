require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Initializer do
  before(:each) do
    Initializer::reset
  end

  it "should have an empty configuration" do
    Initializer::configuration.should be_an_instance_of(Configuration)
  end
end

