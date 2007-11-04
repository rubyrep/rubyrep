require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

CONFIG_FILE = File.dirname(__FILE__) + '/../config/test_config.rb'

include RR

describe Session do
  before(:each) do
    Initializer.reset
    load CONFIG_FILE
  end

  it "initialize should make a deep copy of the Configuration object" do
    ActiveRecord::Base.should_receive(:establish_connection).twice
    ActiveRecord::Base.should_receive(:connection).twice

    session = Session.new
    session.configuration.left.should == Initializer.configuration.left
    session.configuration.right.should == Initializer.configuration.right
    
    Initializer.configuration.left[:dummy] = :dummy_value
    session.configuration.left.has_key?(:dummy).should be_false
  end
  
  it "initialize should establish the database connections" do
    ActiveRecord::Base.should_receive(:establish_connection).twice
    ActiveRecord::Base.should_receive(:connection).twice
    
    session = Session.new
  end
  
  it "initialize shouldn't create the same database connection twice" do
    ActiveRecord::Base.should_receive(:establish_connection).once
    ActiveRecord::Base.should_receive(:connection).once
    
    Initializer.configuration.right = Initializer.configuration.left.clone
    
    session = Session.new
  end  
end

