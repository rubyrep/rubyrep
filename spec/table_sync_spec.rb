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
  
  it "sync_options should return the correct table specific sync options" do
    config = standard_config
    old_table_specific_options = config.table_specific_options
    begin
      config.sync_options = {:syncer => :bla}
      config.add_options_for_table 'scanner_records', :sync_options => {:syncer => :blub}
      TableSync.new(Session.new(config), 'scanner_records').sync_options[:syncer] \
        .should == :blub
    ensure
      config.instance_eval {@table_specific_options = old_table_specific_options}
    end
  end
end  
