require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Committers::BufferedCommitter do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself" do
    Committers.committers[:buffered_commit].should == Committers::BufferedCommitter
  end

  # Stubs out the starting of transactions in the given Session.
  def stub_begin_transaction(session)
    session.left.stub! :begin_db_transaction
    session.right.stub! :begin_db_transaction
  end

  # Stubs out the executing of SQL statements for the given Session.
  def stub_execute(session)
    session.left.stub! :execute
    session.right.stub! :execute
  end

  it "trigger_mode_switcher should return and if necessary create the trigger mode switcher" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    switcher = committer.trigger_mode_switcher
    switcher.should be_an_instance_of(TriggerModeSwitcher)

    committer.trigger_mode_switcher.should == switcher # ensure it is only created one
  end

  it "activity_marker_table should return the correct table name" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    committer.activity_marker_table.should == 'rx_active'
  end

  it "maintain_activity_status should return true if activity marker table exists" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    committer.maintain_activity_status?.should be_true
  end

  it "maintain_activity_status should return false if activity marker does not exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    committer.maintain_activity_status?.should be_false
  end

  it "commit_frequency should return the configured commit frequency" do
    config = deep_copy(standard_config)
    config.options[:commit_frequency] = 5
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    committer.commit_frequency.should == 5
  end

  it "commit_frequency should return the the default commit frequency if nothing else is configured" do
    config = deep_copy(standard_config)
    config.options.delete :commit_frequency
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})
    committer.commit_frequency.should == Committers::BufferedCommitter::DEFAULT_COMMIT_FREQUENCY
  end

  it "initialize should start transactions and setup rubyrep activity filtering" do
    session = nil
    begin
      session = Session.new
      session.left.should_receive(:begin_db_transaction)
      session.right.should_receive(:begin_db_transaction)
      session.left.select_one("select * from rr_active").should be_nil # verify starting situation
      committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

      # rubyrep activity should be marked
      session.left.select_one("select * from rr_active").should_not be_nil
      session.right.select_one("select * from rr_active").should_not be_nil

      # the triggers of 'left_table' should have been switched to ignore rubyrep activity
      committer.trigger_mode_switcher.triggers['left_table'].should == {
        :left => 'left_table',
        :right => 'right_table'
      }
    ensure
      session.left.execute "delete from rr_active" if session
      session.right.execute "delete from rr_active" if session
    end
  end

  it "commit_db_transactions should commit the transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_receive(:commit_db_transaction)
    session.right.should_receive(:commit_db_transaction)
    committer.commit_db_transactions
  end

  it "commit_db_transactions should clear the activity marker table" do
    session = Session.new
    stub_begin_transaction session
    session.left.stub!(:commit_db_transaction)
    session.right.stub!(:commit_db_transaction)
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_receive(:execute).with("delete from rr_active")
    session.right.should_receive(:execute).with("delete from rr_active")
    committer.commit_db_transactions
  end

  it "commit_db_transactions should not clear the activity marker table if it doesn't exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new config
    stub_begin_transaction session
    session.left.stub!(:commit_db_transaction)
    session.right.stub!(:commit_db_transaction)
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_not_receive(:execute)
    session.right.should_not_receive(:execute)
    committer.commit_db_transactions
  end

  it "begin_db_transactions should begin new transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_receive(:begin_db_transaction)
    session.right.should_receive(:begin_db_transaction)
    committer.begin_db_transactions
  end

  it "begin_db_transactions should insert a record into the activity marker table" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_receive(:execute).with("insert into rr_active values(1)")
    session.right.should_receive(:execute).with("insert into rr_active values(1)")
    committer.begin_db_transactions
  end

  it "begin_db_transactions should not clear the activity marker table if it doesn't exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_not_receive(:execute)
    session.right.should_not_receive(:execute)
    committer.begin_db_transactions
  end

  it "rollback_db_transactions should roll back the transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.left.should_receive(:rollback_db_transaction)
    session.right.should_receive(:rollback_db_transaction)
    committer.rollback_db_transactions
  end

  it "commit should only commit and start new transactions if the specified number of changes have been executed" do
    config = deep_copy(standard_config)
    config.options[:commit_frequency] = 2
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    committer.should_not_receive(:commit_db_transactions).twice
    committer.should_not_receive(:begin_db_transactions).twice
    4.times {committer.commit}
  end

  it "insert_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.right.should_receive(:insert_record).with('right_table', :dummy_values)
    committer.should_receive(:commit)
    
    committer.insert_record(:right, :dummy_values)
  end

  it "update_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.right.should_receive(:update_record).with('right_table', :dummy_values)
    committer.should_receive(:commit)

    committer.update_record(:right, :dummy_values)
  end

  it "delete_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    session.right.should_receive(:delete_record).with('right_table', :dummy_values)
    committer.should_receive(:commit)

    committer.delete_record(:right, :dummy_values)
  end

  it "finalize should commit the transactions if called with success = true" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    committer.should_receive(:commit_db_transactions)

    committer.finalize true
  end

  it "finalize should commit the transactions if called with success = true" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session, 'left_table', 'right_table', {})

    committer.should_receive(:rollback_db_transactions)

    committer.finalize false
  end
end

