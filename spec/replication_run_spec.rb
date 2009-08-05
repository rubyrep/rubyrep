require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationRun do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should store the provided session" do
    session = Session.new
    sweeper = TaskSweeper.new 1
    run = ReplicationRun.new session, sweeper
    run.session.should == session
  end

  it "install_sweeper should install a task sweeper into the database connections" do
    session = Session.new
    sweeper = TaskSweeper.new 1
    run = ReplicationRun.new session, sweeper
    run.install_sweeper

    session.left.sweeper.should == sweeper
    session.right.sweeper.should == sweeper
  end

  it "helper should return the correctly initialized replication helper" do
    run = ReplicationRun.new Session.new, TaskSweeper.new(1)
    run.helper.should be_an_instance_of(ReplicationHelper)
    run.helper.replication_run.should == run
    run.helper.should == run.helper # ensure the helper is created only once
  end

  it "replicator should return the configured replicator" do
    session = Session.new
    run = ReplicationRun.new session, TaskSweeper.new(1)
    run.replicator.
      should be_an_instance_of(Replicators.replicators[session.configuration.options[:replicator]])
    run.replicator.should == run.replicator # should only create the replicator once
    run.replicator.rep_helper.should == run.helper
  end

  it "run should replicate all logged changes" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit

      session = Session.new(config)

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.run

      session.right.select_one("select * from extender_no_record").should == {
        'id' => '1',
        'name' => 'bla'
      }
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from extender_no_record"
        session.right.execute "delete from extender_no_record"
        session.left.execute "delete from rr_pending_changes"
      end
    end
  end

  it "run should not create the replicator if there are no pending changes" do
    session = Session.new
    run = ReplicationRun.new session, TaskSweeper.new(1)
    run.should_not_receive(:replicator)
    run.run
  end

  it "run should only replicate real differences" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.replicator.should_not_receive(:replicate)
      run.run

    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "run should log raised exceptions" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.replicator.stub!(:replicate_difference).and_return {raise Exception, 'dummy message'}
      run.run

      row = session.left.select_one("select * from rr_logged_events")
      row['description'].should == 'dummy message'
      row['long_description'].should =~ /Exception/
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "run should re-raise original exception if logging to database fails" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.replicator.stub!(:replicate_difference).and_return {raise Exception, 'dummy message'}
      run.helper.stub!(:log_replication_outcome).and_return {raise Exception, 'blub'}
      lambda {run.run}.should raise_error(Exception, 'dummy message')
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "run should return silently if timed out before work actually started" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      sweeper = TaskSweeper.new(1)
      sweeper.stub!(:terminated?).and_return(true)
      run = ReplicationRun.new session, sweeper
      LoggedChangeLoaders.should_not_receive(:new)
      run.run
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "run should rollback if timed out" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      sweeper = TaskSweeper.new(1)
      sweeper.should_receive(:terminated?).any_number_of_times.and_return(false, true)
      run = ReplicationRun.new session, sweeper
      run.helper.should_receive(:finalize).with(false)
      run.run
    ensure
      session.left.rollback_db_transaction if session.left
      session.right.rollback_db_transaction if session.right
    end
  end

  it "run should not catch exceptions raised during replicator initialization" do
    config = deep_copy(standard_config)
    config.options[:logged_replication_events] = [:invalid_option]
    session = Session.new config
    session.left.begin_db_transaction
    begin

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      lambda {run.run}.should raise_error(ArgumentError)
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "run should process trigger created change log records" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:logged_replication_events] = [:all_changes]

      session = Session.new(config)
      session.left.execute "delete from rr_logged_events"
      initializer = ReplicationInitializer.new(session)
      initializer.create_trigger :left, 'extender_no_record'

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.run

      session.right.select_one("select * from extender_no_record").should == {
        'id' => '1',
        'name' => 'bla'
      }

      # also verify that event was logged
      row = session.left.select_one("select * from rr_logged_events")
      row['diff_type'].should == 'left'
      row['change_key'].should == '1'
      row['description'].should == 'replicated'
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from extender_no_record"
        session.right.execute "delete from extender_no_record"
        session.left.execute "delete from rr_pending_changes"
      end
      initializer.drop_trigger :left, 'extender_no_record' if initializer
    end
  end
end