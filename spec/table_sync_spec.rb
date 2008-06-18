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
  
  it "sync_options should return the correct sync options when there are no table specific options" do
    table_sync = TableSync.new Session.new(standard_config), "scanner_records"
    table_sync.session.configuration.stub!(:sync_options).and_return({
        :dummy_a => 1, :dummy_b => 2})
    table_sync.sync_options.should == {
      :dummy_a => 1, :dummy_b => 2}
  end
  
  it "sync_options should return the correct sync options when there are table specific options as string" do
    table_sync = TableSync.new Session.new(standard_config), "scanner_records"
    table_sync.session.configuration.stub!(:sync_options).and_return({
        :dummy_a => 1, :dummy_b => 2, 
        :table_specific => [
          {"a" => {:dummy_c => 3}}, 
          {"scanner_records" => {:dummy_a => 10, :dummy_d => 4}}]})
    table_sync.sync_options.should == {
      :dummy_a => 10, :dummy_b => 2, :dummy_d => 4}
  end
  
  it "sync_options should return the correct sync options when there are table specific options as regexp" do
    table_sync = TableSync.new Session.new(standard_config), "scanner_records"
    table_sync.session.configuration.stub!(:sync_options).and_return({
        :dummy_a => 1, :dummy_b => 2, 
        :table_specific => [
          {/other_table./ => {:dummy_c => 3}},
          {/scanner_record./ => {:dummy_a => 55, :dummy_d => 4}},
          {/scanner_record./ => {:dummy_a => 10, :dummy_e => 5}}]})
    table_sync.sync_options.should == {
      :dummy_a => 10, :dummy_b => 2, :dummy_d => 4, :dummy_e => 5}
  end
  
  it "sync_options should complain if any table hash contains more than 1 element" do
    table_sync = TableSync.new Session.new(standard_config), "scanner_records"
    table_sync.session.configuration.stub!(:sync_options).and_return({
        :dummy_a => 1, :dummy_b => 2, 
        :table_specific => [
          /other_table./ => {:dummy_c => 3},
          /scanner_record./ => {:dummy_a => 55, :dummy_d => 4}]})
    lambda {table_sync.sync_options}.should raise_error(RuntimeError, /table_specific.*multiple entries/)
  end
end  
