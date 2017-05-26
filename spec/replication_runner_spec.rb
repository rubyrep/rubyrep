require 'spec_helper'

include RR

describe ReplicationRunner do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['replicate'][:command].should == ReplicationRunner
    CommandRunner.commands['replicate'][:description].should be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).at_least(1).times
    runner = ReplicationRunner.new
    status = runner.process_options ["--nonsense"]
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).at_least(1).times
    runner = ReplicationRunner.new
    status = runner.process_options []
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    runner = ReplicationRunner.new
    status = runner.process_options ["--help"]
    runner.options.should == nil
    status.should == 0
  end

  it "process_options should set the correct options" do
    runner = ReplicationRunner.new
    runner.process_options ["-c", "config_path"]
    runner.options[:config_file].should == 'config_path'
  end

  it "run should not start a replication if the command line is invalid" do
    $stderr.should_receive(:puts).at_least(1).times
    expect_any_instance_of(ReplicationRunner).to_not receive(:execute)
    ReplicationRunner.run(["--nonsense"])
  end

  it "run should start a replication if the command line is correct" do
    expect_any_instance_of(ReplicationRunner).to receive(:execute)
    ReplicationRunner.run(["--config=path"])
  end

  it "session should create and return the session" do
    runner = ReplicationRunner.new
    runner.options = {:config_file => "config/test_config.rb"}
    runner.session.should be_an_instance_of(Session)
    runner.session.should == runner.session # should only be created one time
  end

  it "pause_replication should not pause if next replication is already overdue" do
    runner = ReplicationRunner.new
    runner.stub(:session).and_return(Session.new(standard_config))
    waiter_thread = double('waiter_thread')
    waiter_thread.should_not_receive(:join)
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    runner.pause_replication # verify no wait during first run
    runner.instance_variable_set(:@last_run, 1.hour.ago)
    runner.pause_replication # verify no wait if overdue
  end

  it "pause_replication should pause for correct time frame" do
    runner = ReplicationRunner.new
    runner.stub(:session).and_return(Session.new(deep_copy(standard_config)))
    runner.session.configuration.stub(:options).and_return(:replication_interval => 2)
    waiter_thread = double('waiter_thread')
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    now = Time.now
    Time.stub(:now).and_return(now)
    runner.instance_variable_set(:@last_run, now - 1.seconds)
    waiter_thread.should_receive(:join) {|time| time.to_f.should be_within(0.1).of(1.0); 0}

    runner.pause_replication
  end

  it "init_waiter should setup correct signal processing" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      runner = ReplicationRunner.new
      runner.stub(:session).and_return(Session.new(standard_config))
    
      # simulate sending the TERM signal
      Signal.should_receive(:trap).with('TERM').and_yield

      # also verify that the INT signal is trapped
      Signal.should_receive(:trap).with('INT')

      runner.init_waiter

      # verify the that any pause would have been prematurely finished and
      # termination signal been set
      is_alive = nil
      10.times do
        is_alive = runner.instance_variable_get(:@waiter_thread).alive?
        break unless is_alive
        sleep 0.1
      end
      is_alive.should be false
      runner.instance_variable_get(:@waiter_thread).should_not be_alive
      runner.termination_requested.should be true
      $stdout.string.should =~ /TERM.*shutdown/
    ensure
      $stdout = org_stdout
    end
  end

  it "prepare_replication should call ReplicationInitializer#prepare_replication" do
    runner = ReplicationRunner.new
    runner.stub(:session).and_return(:dummy_session)
    initializer  = double('replication_initializer')
    initializer.should_receive(:prepare_replication)
    ReplicationInitializer.should_receive(:new).with(:dummy_session).and_return(initializer)
    runner.prepare_replication
  end

  it "execute should catch and print exceptions" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      session = Session.new
      runner = ReplicationRunner.new
      runner.stub(:session).and_return(session)
      runner.stub(:init_waiter)
      runner.stub(:prepare_replication)
      runner.stub(:pause_replication)
      runner.should_receive(:termination_requested).twice.and_return(false, true)

      session.should_receive(:refresh) {raise "refresh failed"}

      runner.execute
      
      $stderr.string.should =~ /Exception caught.*refresh failed/
      $stderr.string.should =~ /replication_runner.rb:[0-9]+:in/
    ensure
      $stderr = org_stderr
    end
  end

  it "execute_once should not clean up if successful" do
    runner = ReplicationRunner.new
    session = Session.new
    runner.instance_variable_set(:@session, session)

    runner.execute_once
    runner.instance_variable_get(:@session).should == session
  end

  it "execute_once should clean up after failed replication runs" do
    runner = ReplicationRunner.new
    session = Session.new
    runner.instance_variable_set(:@session, session)

    session.should_receive(:refresh).and_raise('bla')
    lambda {runner.execute_once}.should raise_error('bla')
    runner.instance_variable_get(:@session).should be_nil
  end

  it "execute_once should raise exception if replication run times out" do
    session = Session.new
    runner = ReplicationRunner.new
    runner.stub(:session).and_return(session)
    terminated = double("terminated")
    terminated.stub(:terminated?).and_return(true)
    TaskSweeper.stub(:timeout).and_return(terminated)

    lambda {runner.execute_once}.should raise_error(/timed out/)
  end

  def replication_config
    config = deep_copy(standard_config)
    config.options[:committer] = :buffered_commit
    config.options[:replication_interval] = 0.01

    # reset table selection
    config.included_table_specs.replace ['scanner_left_records_only']
    config.tables_with_options.clear

    config
  end

  def prepare_replication_data
    session = Session.new replication_config
    initializer = ReplicationInitializer.new session
    [:left, :right].each do |database|
      initializer.clear_sequence_setup database, 'scanner_left_records_only'
      if initializer.trigger_exists?(database, 'scanner_left_records_only')
        initializer.drop_trigger database, 'scanner_left_records_only'
      end
      session.send(database).execute "delete from scanner_left_records_only where name = 'bla'"
    end
    session.right.execute "delete from scanner_left_records_only"
  end

  it "execute should start the replication" do
    prepare_replication_data

    session = Session.new replication_config
    org_stdout = $stdout
    begin
      $stdout = StringIO.new
      runner = ReplicationRunner.new
      runner.process_options ["-c", "./config/test_config.rb"]
      runner.stub(:session).and_return(session)

      runner.should_receive(:replication_preparation_finished) do
        record = session.right.select_record query: "select * from scanner_left_records_only where id = 1", table: :scanner_left_records_only
        record['name'].should == 'Alice'

        # to prepare for replication test
        session.left.execute "insert into scanner_left_records_only(name) values('bla')"
      end

      runner.should_receive(:replication_run_finished) do
        record = session.right.select_record query: "select * from scanner_left_records_only where name = 'bla'", table: :scanner_left_records_only
        record['name'].should == 'bla'
        runner.instance_variable_set(:@termination_requested, true)
      end

      runner.execute
    ensure
      $stdout = org_stdout
      prepare_replication_data
    end
  end
end