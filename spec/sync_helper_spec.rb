require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should initialize the correct committer" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should be_an_instance_of(Committers::DefaultCommitter)
    c.session.should == helper.session
    c.tables.should == {:left => helper.left_table, :right =>helper.right_table}
    c.sync_options.should == helper.sync_options
  end

  it "session should return the session" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.session.should == sync.session
  end

  it "left_table and right_table should return the correct table names" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.left_table.should == 'scanner_records'
    helper.right_table.should == 'scanner_records'

    sync = TableSync.new(Session.new, 'scanner_records', 'right_table')
    helper = SyncHelper.new(sync)
    helper.left_table.should == 'scanner_records'
    helper.right_table.should == 'right_table'
  end

  it "table_sync should return the current table sync instance" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.table_sync.should == sync
  end

  it "sync_options should return the correct sync options" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.sync_options.should == sync.sync_options
  end

  it "insert_record should insert the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:insert_record).with(:right, :dummy_record)
    helper.insert_record :right, :dummy_record
  end

  it "update_record should update the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:update_record).with(:right, :dummy_record)
    helper.update_record :right, :dummy_record
  end

  it "delete_record should delete the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:delete_record).with(:right, :dummy_record)
    helper.delete_record :right, :dummy_record
  end
end