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
    helper.should_receive(:delete_record).with(:left, :dummy_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:left, :dummy_record)

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right, :delete => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:delete_record).with(:right, :dummy_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it "sync_difference should not insert if :no_insert option is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :no_insert => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it "sync_difference should insert in the right database" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_receive(:insert_record).with(:left, :dummy_record)
    syncer.sync_difference(:right, :dummy_record)

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:insert_record).with(:right, :dummy_record)
    syncer.sync_difference(:left, :dummy_record)
  end

  it "sync_difference should not update if :no_update is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left, :no_update => true})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:conflict, :dummy_records)
  end

  it "sync_difference should update the right values in the right database" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :left})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_receive(:update_record).with(:left, :right_record)
    helper.should_not_receive(:insert_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])

    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return({:direction => :right})
    syncer = Syncers::OneWaySyncer.new(helper)
    helper.should_receive(:update_record).with(:right, :left_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])
  end
end