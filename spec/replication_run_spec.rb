require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationRun do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should store the provided session" do
    session = Session.new
    run = ReplicationRun.new session
    run.session.should == session
  end

  it "helper should return the correctly initialized replication helper" do
    run = ReplicationRun.new Session.new
    run.helper.should be_an_instance_of(ReplicationHelper)
    run.helper.replication_run.should == run
    run.helper.should == run.helper # ensure the helper is created only once
  end

  it "replicator should return the configured replicator" do
    session = Session.new
    run = ReplicationRun.new session
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
      session.left.insert_record 'rr_change_log', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session
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
        session.left.execute "delete from rr_change_log"
      end
    end
  end

  it "run should only replicate real differences" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin

      session.left.insert_record 'rr_change_log', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.right.insert_record 'rr_change_log', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session
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

      session.left.insert_record 'rr_change_log', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      run = ReplicationRun.new session
      run.replicator.stub!(:replicate_difference).and_return {raise Exception, 'dummy message'}
      run.run

      row = session.left.select_one("select * from rr_event_log")
      row['description'].should == 'dummy message'
      row['long_description'].should =~ /Exception/
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "run should not catch exceptions raised during replicator initialization" do
    config = deep_copy(standard_config)
    config.options[:logged_replication_events] = [:invalid_option]
    run = ReplicationRun.new Session.new(config)
    lambda {run.run}.should raise_error(ArgumentError)
  end

  it "run should process trigger created change log records" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:logged_replication_events] = [:all_changes]

      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)
      initializer.create_trigger :left, 'extender_no_record'

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }

      run = ReplicationRun.new session
      run.run

      session.right.select_one("select * from extender_no_record").should == {
        'id' => '1',
        'name' => 'bla'
      }

      # also verify that event was logged
      row = session.left.select_one("select * from rr_event_log")
      row['diff_type'].should == 'left'
      row['change_key'].should == '1'
      row['description'].should == 'replicated'
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from extender_no_record"
        session.right.execute "delete from extender_no_record"
        session.left.execute "delete from rr_change_log"
      end
      initializer.drop_trigger :left, 'extender_no_record' if initializer
    end
  end
end