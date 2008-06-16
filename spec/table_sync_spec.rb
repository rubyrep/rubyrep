require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSync do
  before(:each) do
    @old_syncers = TableSync.send :class_variable_get, :@@syncers rescue nil
    TableSync.send :class_variable_set, :@@syncers, nil
  end

  after(:each) do
    TableSync.send :class_variable_set, :@@syncers, @old_syncers
  end
  
  it "syncers should return empty hash if empty" do
    TableSync.syncers.should == {}
  end
  
  it "register_syncer should register, syncer return the registerred syncers" do
    TableSync.register_syncer :key1 => :dummy_syncer1
    TableSync.register_syncer :key2 => :dummy_syncer2
    TableSync.syncers.should == {:key1 => :dummy_syncer1, :key2 => :dummy_syncer2}
  end
  
end  
