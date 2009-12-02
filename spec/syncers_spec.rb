require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Syncers do
  before(:each) do
    @old_syncers = Syncers.syncers
  end

  after(:each) do
    Syncers.instance_variable_set :@syncers, @old_syncers
  end

  it "syncers should return empty hash if nil" do
    Syncers.instance_variable_set :@syncers, nil
    Syncers.syncers.should == {}
  end

  it "syncers should return the registered syncers" do
    Syncers.instance_variable_set :@syncers, :dummy_data
    Syncers.syncers.should == :dummy_data
  end

  it "configured_syncer should return the correct syncer as per :syncer option, if both :syncer and :replicator is configured" do
    options = {
      :syncer => :two_way,
      :replicator => :key2
    }
    Syncers.configured_syncer(options).should == Syncers::TwoWaySyncer
  end

  it "configured_syncer should return the correct syncer as per :replicator option if no :syncer option is provided" do
    options = {:replicator => :two_way}
    Syncers.configured_syncer(options).should == Syncers::TwoWaySyncer
  end

  it "register should register the provided commiter" do
    Syncers.instance_variable_set :@syncers, nil
    Syncers.register :a_key => :a
    Syncers.register :b_key => :b
    Syncers.syncers[:a_key].should == :a
    Syncers.syncers[:b_key].should == :b
  end
end

describe Syncers::OneWaySyncer do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself" do
    Syncers::syncers[:one_way].should == Syncers::OneWaySyncer
  end

  it "initialize should store sync_helper" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    syncer = Syncers::OneWaySyncer.new(helper)
    syncer.sync_helper.should == helper
  end

  it "initialize should calculate course source, target and source_record_index" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)

    # verify correct behaviour if syncing to the left
    helper.stub!(:sync_options).and_return({:direction => :left})
    syncer = Syncers::OneWaySyncer.new(helper)
    syncer.source.should == :right
    syncer.target.should == :left
    syncer.source_record_index.should == 1

    # verify correct behaviour if syncing to the right
    helper.stub!(:sync_options).and_return({:direction => :right})
    syncer = Syncers::OneWaySyncer.new(helper)
    syncer.source.should == :left
    syncer.target.should == :right
    syncer.source_record_index.should == 0
  end

  it "default_option should return the correct default options" do
    Syncers::OneWaySyncer.default_options.should == {
      :direction => :right,
      :delete => false, :update => true, :insert => true
    }
  end

  it "sync_difference should only delete if :delete option is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:left, :dummy_record)
  end

  it "sync_difference should delete in the right database" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :delete => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:delete_record).with(:left, 'scanner_records', :dummy_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:left, :dummy_record)

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right, :delete => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it "sync_difference should not insert if :insert option is not true" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :insert => false})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it "sync_difference should insert in the right database" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :insert => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_receive(:insert_record).with(:left, 'scanner_records', :dummy_record)
    syncer.sync_difference(:right, :dummy_record)

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right, :insert => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    syncer.sync_difference(:left, :dummy_record)
  end

  it "sync_difference should not update if :update option is not true" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :update => false})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:conflict, :dummy_records)
  end

  it "sync_difference should update the right values in the right database" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :update => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_receive(:update_record).with(:left, 'scanner_records', :right_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right, :update => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:update_record).with(:right, 'scanner_records', :left_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])
  end
end