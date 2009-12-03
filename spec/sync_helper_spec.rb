require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe SyncHelper do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should initialize the correct committer" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should be_an_instance_of(Committers::DefaultCommitter)
    c.session.should == helper.session
  end

  it "session should return the session" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.session.should == sync.session
  end

  it "extract_key should extract the primary key column_name => value pairs" do
    sync = TableSync.new(Session.new, 'extender_combined_key')
    helper = SyncHelper.new(sync)
    helper.extract_key('first_id' => 1, 'second_id' => 2, 'name' => 'bla').
      should == {'first_id' => 1, 'second_id' => 2}
  end

  it "ensure_event_log should ask the replication_initializer to ensure the event log" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    ReplicationInitializer.any_instance_should_receive(:ensure_event_log) do
      helper.ensure_event_log
    end
  end

  it "log_sync_outcome should log the replication outcome correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      sync = TableSync.new(Session.new, 'scanner_records')
      helper = SyncHelper.new(sync)

      # Verify that the log information are made fitting
      helper.should_receive(:fit_description_columns).
        with('my_outcome', 'my_long_description').
        and_return(['my_outcomeX', 'my_long_descriptionY'])

      helper.log_sync_outcome(
        {'bla' => 'blub', 'id' => 1},
        'my_sync_type',
        'my_outcome',
        'my_long_description'
      )
      
      row = session.left.select_one("select * from rr_logged_events order by id desc")
      row['activity'].should == 'sync'
      row['change_table'].should == 'scanner_records'
      row['diff_type'].should == 'my_sync_type'
      row['change_key'].should == '1'
      row['left_change_type'].should be_nil
      row['right_change_type'].should be_nil
      row['description'].should == 'my_outcomeX'
      row['long_description'].should == 'my_long_descriptionY'
      Time.parse(row['event_time']).should >= 10.seconds.ago
      row['diff_dump'].should == nil
    ensure
      session.left.rollback_db_transaction if session
    end
  end

  it "log_sync_outcome should log events for combined primary key tables correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      sync = TableSync.new(Session.new, 'extender_combined_key')
      helper = SyncHelper.new(sync)

      helper.log_sync_outcome(
        {'bla' => 'blub', 'first_id' => 1, 'second_id' => 2},
        'my_sync_type',
        'my_outcome',
        'my_long_description'
      )

      row = session.left.select_one("select * from rr_logged_events order by id desc")
      row['change_key'].should == '"first_id"=>"1", "second_id"=>"2"'
    ensure
      session.left.rollback_db_transaction if session
    end
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

  it "tables should return the correct table name hash" do
    sync = TableSync.new(Session.new, 'scanner_records', 'right_table')
    helper = SyncHelper.new(sync)
    helper.tables.should == {:left => 'scanner_records', :right => 'right_table'}
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
    c.should_receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    helper.insert_record :right, 'scanner_records', :dummy_record
  end

  it "update_record should update the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:update_record).with(:right, 'scanner_records', :dummy_record, nil)
    helper.update_record :right, 'scaner_records', :dummy_record
  end

  it "update_record should update the given record with the provided old key" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:update_record).with(:right, 'scanner_records', :dummy_record, :old_key)
    helper.update_record :right, 'scanner_records', :dummy_record, :old_key
  end

  it "delete_record should delete the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    c.should_receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    helper.delete_record :right, 'scanner_records', :dummy_record
  end

  it "finalize should be delegated to the committer" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)

    # finalize itself should not lead to creation of committer
    helper.finalize
    helper.instance_eval {@committer}.should be_nil
    
    c = helper.instance_eval {committer}
    c.should_receive(:finalize).with(false)
    helper.finalize(false)
  end
end