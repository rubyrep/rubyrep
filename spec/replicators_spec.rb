require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Replicators do
  before(:each) do
    @old_replicators = Replicators.replicators
  end

  after(:each) do
    Replicators.instance_variable_set :@replicators, @old_replicators
  end

  it "replicators should return empty hash if nil" do
    Replicators.instance_variable_set :@replicators, nil
    Replicators.replicators.should == {}
  end

  it "replicators should return the registered replicators" do
    Replicators.instance_variable_set :@replicators, :dummy_data
    Replicators.replicators.should == :dummy_data
  end

  it "configured_replicator should return the correct replicator" do
    options = {:replicator => :two_way}
    Replicators.configured_replicator(options).should == Replicators::TwoWayReplicator
  end
  
  it "register should register the provided replicator" do
    Replicators.instance_variable_set :@replicators, nil
    Replicators.register :a_key => :a
    Replicators.register :b_key => :b
    Replicators.replicators[:a_key].should == :a
    Replicators.replicators[:b_key].should == :b
  end
end
