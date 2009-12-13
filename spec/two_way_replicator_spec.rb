require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Replicators::TwoWayReplicator do
  before(:each) do
    Initializer.configuration = deep_copy(standard_config)
    Initializer.configuration.options = {:replicator => :two_way}
  end

  it "should register itself" do
    Replicators::replicators[:two_way].should == Replicators::TwoWayReplicator
  end

  it "initialize should store the replication helper" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    replicator.rep_helper.should == helper
  end

  it "verify_option should raise descriptive errors" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    lambda {replicator.verify_option(nil, [:valid_value], :my_key, :my_value)}.
      should raise_error(ArgumentError, ':my_value not a valid :my_key option')
    lambda {replicator.verify_option(/my_spec/, [:valid_value], :my_key, :my_value)}.
      should raise_error(ArgumentError, '/my_spec/: :my_value not a valid :my_key option')
  end

  it "initialize should throw an error if options are invalid" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    base_options = {
      :replicator => :two_way,
      :left_change_handling => :ignore,
      :right_change_handling => :ignore,
      :replication_conflict_handling => :ignore,
      :logged_replication_events => [:ignored_conflicts]
    }

    # Verify that correct options don't raise errors.
    helper.stub!(:options).and_return(base_options)
    lambda {Replicators::TwoWayReplicator.new(helper)}.should_not raise_error

    # Also lambda options should not raise errors.
    l = lambda {}
    helper.stub!(:options).and_return(base_options.merge(
        {
          :left_change_handling => l,
          :right_change_handling => l,
          :repliction_conflict_handling => l
        })
    )
    lambda {Replicators::TwoWayReplicator.new(helper)}.should_not raise_error

    # Invalid options should raise errors
    invalid_options = [
      {:left_change_handling => :invalid_left_option},
      {:right_change_handling => :invalid_right_option},
      {:replication_conflict_handling => :invalid_conflict_option},
      {:logged_replication_events => :invalid_logging_option},
    ]
    invalid_options.each do |options|
      helper.session.configuration.stub!(:options).and_return(base_options.merge(options))
      lambda {Replicators::TwoWayReplicator.new(helper)}.should raise_error(ArgumentError)
    end
  end

  it "clear_conflicts should update the correct database with the correct action" do
    Initializer.configuration.include_tables 'left_table, right_table'
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      loaders = LoggedChangeLoaders.new(session)

      left_change = LoggedChange.new loaders[:left]
      left_change.table = 'left_table'
      left_change.key = {'id' => '1'}
      right_change = LoggedChange.new loaders[:right]
      right_change.table = 'right_table'
      right_change.key = {'id' => '1'}

      diff = ReplicationDifference.new(loaders)
      diff.changes[:left] = left_change
      diff.changes[:right] = right_change


      # verify that an insert is dealt correctly with
      left_change.type = :insert
      right_change.type = :insert

      helper.should_receive(:load_record).ordered.
        with(:left, 'left_table', {'id' => '1'}).
        and_return(:dummy_values)
      helper.should_receive(:update_record).ordered.
        with(:right, 'right_table', :dummy_values, {'id' => '1'})
      replicator.clear_conflict :left, diff, 1

      # verify that an update is dealt correctly with
      left_change.type = :delete
      right_change.type = :update
      right_change.new_key = {'id' => '2'}


      helper.should_receive(:load_record).ordered.
        with(:right, 'right_table', {'id' => '2'}).
        and_return(:dummy_values)
      helper.should_receive(:insert_record).ordered.
        with(:left, 'left_table', :dummy_values)
      replicator.clear_conflict :right, diff, 1

    
      # verify that a delete is dealt correctly with
      left_change.type = :delete
      right_change.type = :update

      helper.should_receive(:delete_record).ordered.
        with(:right, 'right_table', {'id' => '2'})
      replicator.clear_conflict :left, diff, 1
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "log_replication_outcome should log conflicts correctly" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new loaders
    diff.type = :conflict
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # should only log events if so configured
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_not_receive(:log_replication_outcome)
    helper.stub!(:options_for_table).and_return({:logged_replication_events => []})
    replicator.log_replication_outcome :ignore, diff
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:ignored_conflicts]})
    replicator.log_replication_outcome :left, diff

    # should log ignored conflicts correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_receive(:log_replication_outcome).with(diff, 'ignored')
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:ignored_conflicts]})
    replicator.log_replication_outcome :ignore, diff

    # should log conflicts correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_receive(:log_replication_outcome).with(diff, 'left_won')
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:all_conflicts]})
    replicator.log_replication_outcome :left, diff
  end

  it "log_replication_outcome should log changes correctly" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new loaders
    diff.type = :left
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # should only log events if so configured
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_not_receive(:log_replication_outcome)
    helper.stub!(:options_for_table).and_return({:logged_replication_events => []})
    replicator.log_replication_outcome :ignore, diff
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:ignored_changes]})
    replicator.log_replication_outcome :left, diff

    # should log changes correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_receive(:log_replication_outcome).with(diff, 'replicated')
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:all_changes]})
    replicator.log_replication_outcome :right, diff

    # should log changes correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.should_receive(:log_replication_outcome).with(diff, 'ignored')
    helper.stub!(:options_for_table).and_return({:logged_replication_events => [:ignored_changes]})
    replicator.log_replication_outcome :ignore, diff
  end

  it "replicate_difference should not do anything if ignore option is given" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return(
      {
        :left_change_handling => :ignore,
        :right_change_handling => :ignore,
        :replication_conflict_handling => :ignore,
        :logged_replication_events => [:ignored_changes, :ignored_conflicts]
      }
    )

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new(loaders)
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # but logging should still happen
    replicator.should_receive(:log_replication_outcome).
      with(:ignore, diff).
      exactly(3).times

    helper.should_not_receive :insert_record
    helper.should_not_receive :update_record
    helper.should_not_receive :delete_record

    diff.type = :conflict
    replicator.replicate_difference diff
    diff.type = :left
    replicator.replicate_difference diff
    diff.type = :right
    replicator.replicate_difference diff
  end

  it "replicate_difference should call the provided Proc objects" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    lambda_parameters = []
    l = lambda do |rep_helper, diff|
      lambda_parameters << [rep_helper, diff]
    end
    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return(
      {
        :left_change_handling => l,
        :right_change_handling => l,
        :replication_conflict_handling => l
      }
    )

    loaders = LoggedChangeLoaders.new(session)

    change = LoggedChange.new loaders[:left]
    change.table = 'scanner_records'

    d1 = ReplicationDifference.new(loaders)
    d1.type = :conflict
    d1.changes[:left] = change
    replicator.replicate_difference d1

    d2 = ReplicationDifference.new(loaders)
    d2.type = :left
    d2.changes[:left] = change
    replicator.replicate_difference d2

    d3 = ReplicationDifference.new(loaders)
    d3.type = :right
    d3.changes[:left] = change
    replicator.replicate_difference d3

    lambda_parameters.should == [
      [helper, d1],
      [helper, d2],
      [helper, d3],
    ]
  end

  it "replicate_difference should clear conflicts as per provided options" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    left_change = LoggedChange.new LoggedChangeLoader.new(session, :left)
    left_change.table = 'scanner_records'
    right_change = LoggedChange.new LoggedChangeLoader.new(session, :right)
    right_change.table = 'scanner_records'
    diff = ReplicationDifference.new(session)
    diff.type = :conflict
    diff.changes[:left] = left_change
    diff.changes[:right] = right_change

    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return({:replication_conflict_handling => :left_wins})
    replicator.should_receive(:clear_conflict).with(:left, diff, 1)
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return({:replication_conflict_handling => :right_wins})
    replicator.should_receive(:clear_conflict).with(:right, diff, 1)
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return({:replication_conflict_handling => :later_wins})
    replicator.should_receive(:clear_conflict).with(:left, diff, 1).twice
    left_change.last_changed_at = 5.seconds.from_now
    right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    left_change.last_changed_at = right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    replicator.should_receive(:clear_conflict).with(:right, diff, 1)
    right_change.last_changed_at = 5.seconds.from_now
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    helper.stub!(:options_for_table).and_return({:replication_conflict_handling => :earlier_wins})
    replicator.should_receive(:clear_conflict).with(:left, diff, 1).twice
    left_change.last_changed_at = 5.seconds.ago
    right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    left_change.last_changed_at = right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    replicator.should_receive(:clear_conflict).with(:right, diff, 1)
    right_change.last_changed_at = 5.seconds.ago
    replicator.replicate_difference diff, 1
  end

  it "replicate_difference should replicate :left / :right changes correctly" do
    Initializer.configuration.include_tables 'left_table, right_table'
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      rep_run = ReplicationRun.new(session, TaskSweeper.new(1))

      left_change = LoggedChange.new LoggedChangeLoader.new(session, :left)
      left_change.table = 'left_table'
      left_change.key = {'id' => '1'}
      right_change = LoggedChange.new LoggedChangeLoader.new(session, :right)
      right_change.table = 'right_table'
      right_change.key = {'id' => '1'}

      diff = ReplicationDifference.new(session)

      # verify insert behaviour
      left_change.type = :insert
      diff.type = :left
      diff.changes[:left] = left_change
      diff.changes[:right] = nil
    
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)
      replicator.should_receive(:log_replication_outcome).with(:left, diff)
      helper.should_receive(:load_record).with(:left, 'left_table', {'id' => '1'}).
        and_return(:dummy_values)
      helper.should_receive(:insert_record).with(:right, 'right_table', :dummy_values)
      replicator.replicate_difference diff

      # verify update behaviour
      right_change.type = :update
      right_change.new_key = {'id' => '2'}
      diff.type = :right
      diff.changes[:right] = right_change

      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)
      replicator.should_receive(:log_replication_outcome).with(:right, diff)
      helper.should_receive(:load_record).with(:right, 'right_table', {'id' => '2'}).
        and_return(:dummy_values)
      helper.should_receive(:update_record).with(:left, 'left_table', :dummy_values, {'id' => '1'})
      replicator.replicate_difference diff

      # verify delete behaviour
      right_change.type = :delete

      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)
      replicator.should_receive(:log_replication_outcome).with(:right, diff)
      helper.should_receive(:delete_record).with(:left, 'left_table', {'id' => '1'})
      replicator.replicate_difference diff
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "replicate_difference should handle inserts failing due duplicate records getting created after the original diff was loaded" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :right_wins

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


      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      session.right.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'blub'
      }
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      replicator.replicate_difference diff, 2

      session.left.select_record(:table => "extender_no_record").should == {
        'id' => 1,
        'name' => 'blub'
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

  it "replicate_difference should handle inserts failing due the new record being deleted after the original diff was loaded" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit

      session = Session.new(config)

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      replicator.replicate_difference diff, 2

      # no rspec expectation: success is when we get till here without exception
    ensure
      Committers::NeverCommitter.rollback_current_session
      session.left.execute "delete from rr_pending_changes" if session
    end
  end

  it "replicate_difference should raise Exception if all replication attempts have been exceeded" do
    rep_run = ReplicationRun.new Session.new, TaskSweeper.new(1)
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    lambda {replicator.replicate_difference :dummy_diff, 0}.
      should raise_error(Exception, "max replication attempts exceeded")
  end
  
  it "replicate_difference should handle updates rejected by the database" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :left_wins

      session = Session.new(config)
      session.left.execute "delete from rr_logged_events"

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      lambda {replicator.replicate_difference diff, 1}.should raise_error(/duplicate/i)

      # Verify that the transaction has not become invalid
      helper.log_replication_outcome diff, "bla", "blub"

      row = session.left.select_one("select * from rr_logged_events")
      row['change_table'].should == 'scanner_records'
      row['change_key'].should == '1'
      row['description'].should == 'bla'
      
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from rr_pending_changes"
        session.left.execute "delete from rr_logged_events"
      end
    end
  end

  it "replicate_difference should handle deletes rejected by the database" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :left_wins

      session = Session.new(config)

      session.left.select_all("select * from rr_logged_events").should == []

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'referenced_table',
        'change_key' => 'first_id|1|second_id|2',
        'change_new_key' => nil,
        'change_type' => 'D',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      lambda {replicator.replicate_difference diff, 1}.should raise_error(/referencing_table_fkey/)

      # Verify that the transaction has not become invalid
      helper.log_replication_outcome diff, "bla", "blub"

      row = session.left.select_one("select * from rr_logged_events")
      row['change_table'].should == 'referenced_table'
      row['change_key'].should =~ /first_id.*1.*second_id.*2/
      row['description'].should == 'bla'

    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from rr_pending_changes"
        session.left.execute "delete from rr_logged_events"
      end
    end
  end

  it "replicate_difference should handle deletes failing due to the target record vanishing" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :left_wins

      session = Session.new(config)

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|3',
        'change_new_key' => nil,
        'change_type' => 'D',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|3',
        'change_new_key' => 'id|4',
        'change_type' => 'U',
        'change_time' => Time.now
      }

      replicator.replicate_difference diff, 2
      
      session.right.select_one("select * from scanner_records where id = 4").
        should be_nil
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from rr_pending_changes"
        session.left.execute "delete from rr_logged_events"
      end
    end
  end

  it "replicate_difference should handle updates failing due to the source record being deleted after the original diff was loaded" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :left_wins

      session = Session.new(config)

      session.left.insert_record 'extender_no_record', {
        'id' => '2',
        'name' => 'bla'
      }
      session.right.insert_record 'extender_no_record', {
        'id' => '2',
        'name' => 'blub'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      session.left.delete_record 'extender_no_record', {'id' => '2'}

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|2',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      replicator.replicate_difference diff, 2

      session.right.select_one("select * from extender_no_record").should be_nil
    ensure
      Committers::NeverCommitter.rollback_current_session
      if session
        session.left.execute "delete from extender_no_record"
        session.right.execute "delete from extender_no_record"
        session.left.execute "delete from rr_pending_changes"
      end
    end
  end

  it "replicate_difference should handle updates failing due to the target record being deleted after the original diff was loaded" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:replication_conflict_handling] = :left_wins

      session = Session.new(config)

      session.left.insert_record 'extender_no_record', {
        'id' => '2',
        'name' => 'bla'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }

      rep_run = ReplicationRun.new session, TaskSweeper.new(1)
      helper = ReplicationHelper.new(rep_run)
      replicator = Replicators::TwoWayReplicator.new(helper)

      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      replicator.replicate_difference diff, 2

      session.right.select_record(:table => "extender_no_record").should == {
        'id' => 2,
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
end