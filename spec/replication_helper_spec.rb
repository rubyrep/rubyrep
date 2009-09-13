require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationHelper do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should initialize the correct committer" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {@committer}
    c.should be_an_instance_of(Committers::DefaultCommitter)
    c.session.should == helper.session
  end

  it "session should return the session" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    helper.session.should == rep_run.session
  end

  it "new_transaction? should delegate to the committer" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {@committer}
    c.should_receive(:new_transaction?).and_return(true)
    helper.new_transaction?.should be_true
  end

  it "replication_run should return the current ReplicationRun instance" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    helper.replication_run.should == rep_run
  end

  it "options should return the correct options" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    helper.options.should == session.configuration.options
  end

  it "insert_record should insert the given record" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {committer}
    c.should_receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    helper.insert_record :right, 'scanner_records', :dummy_record
  end

  it "update_record should update the given record" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {committer}
    c.should_receive(:update_record).with(:right, 'scanner_records', :dummy_record, nil)
    helper.update_record :right, 'scanner_records', :dummy_record
  end

  it "update_record should update the given record with the provided old key" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {committer}
    c.should_receive(:update_record).with(:right, 'scanner_records', :dummy_record, :old_key)
    helper.update_record :right, 'scanner_records', :dummy_record, :old_key
  end

  it "delete_record should delete the given record" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval {committer}
    c.should_receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    helper.delete_record :right, 'scanner_records', :dummy_record
  end

  it "load_record should load the specified record (values converted to original data types)" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    helper.load_record(:right, 'scanner_records', 'id' => '2').should == {
      'id' => 2, # Note: it's a number, not a string...
      'name' => 'Bob - right database version'
    }
  end

  it "log_replication_outcome should log the replication outcome correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
      helper = ReplicationHelper.new(rep_run)

      loaders = LoggedChangeLoaders.new(session)

      left_change = LoggedChange.new loaders[:left]
      right_change = LoggedChange.new loaders[:right]
      diff = ReplicationDifference.new loaders
      diff.changes.replace :left => left_change, :right => right_change
      diff.type = :conflict

      left_change.type, right_change.type = :update, :delete
      left_change.table = right_change.table = 'extender_combined_key'
      left_change.key = right_change.key = {'first_id' => 1, 'second_id' => 2}

      # Verify that the log information are made fitting
      helper.should_receive(:fit_description_columns).
        with('ignore', 'ignored').
        and_return(['ignoreX', 'ignoredY'])

      helper.log_replication_outcome diff, 'ignore', 'ignored'

      row = session.left.select_one("select * from rr_logged_events order by id desc")
      row['activity'].should == 'replication'
      row['change_table'].should == 'extender_combined_key'
      row['diff_type'].should == 'conflict'
      row['change_key'].should == '"first_id"=>"1", "second_id"=>"2"'
      row['left_change_type'].should == 'update'
      row['right_change_type'].should == 'delete'
      row['description'].should == 'ignoreX'
      row['long_description'].should == 'ignoredY'
      Time.parse(row['event_time']).should >= 10.seconds.ago
      row['diff_dump'].should == diff.to_yaml
    ensure
      session.left.rollback_db_transaction if session
    end
  end

  it "finalize should be delegated to the committer" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    c = helper.instance_eval {@committer}
    c.should_receive(:finalize).with(false)
    helper.finalize(false)
  end
end