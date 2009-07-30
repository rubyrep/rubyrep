require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe UninstallRunner do
  before(:each) do
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['uninstall'][:command].should == UninstallRunner
    CommandRunner.commands['uninstall'][:description].should be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = UninstallRunner.new
    status = runner.process_options ["--nonsense"]
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = UninstallRunner.new
    status = runner.process_options []
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    runner = UninstallRunner.new
    status = runner.process_options ["--help"]
    runner.options.should == nil
    status.should == 0
  end

  it "process_options should set the correct options" do
    runner = UninstallRunner.new
    runner.process_options ["-c", "config_path"]
    runner.options[:config_file].should == 'config_path'
  end

  it "run should not start an uninstall if the command line is invalid" do
    $stderr.should_receive(:puts).any_number_of_times
    UninstallRunner.any_instance_should_not_receive(:execute) {
      UninstallRunner.run(["--nonsense"])
    }
  end

  it "run should start an uninstall if the command line is correct" do
    UninstallRunner.any_instance_should_receive(:execute) {
      UninstallRunner.run(["--config=path"])
    }
  end

  it "session should create and return the session" do
    runner = UninstallRunner.new
    runner.options = {:config_file => "config/test_config.rb"}
    runner.session.should be_an_instance_of(Session)
    runner.session.should == runner.session # should only be created one time
  end

  it "execute should uninstall all rubyrep elements" do
    begin
      org_stdout, $stdout = $stdout, StringIO.new
      config = deep_copy(standard_config)
      config.options[:rep_prefix] = 'rx'
      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)

      initializer.ensure_infrastructure
      initializer.create_trigger :left, 'scanner_records'

      runner = UninstallRunner.new
      runner.stub!(:session).and_return(session)

      runner.execute

      initializer.trigger_exists?(:left, 'scanner_records').should be_false
      initializer.change_log_exists?(:left).should be_false
      session.right.tables.include?('rx_running_flags').should be_false
      initializer.event_log_exists?.should be_false

      $stdout.string =~ /uninstall completed/i
    ensure
      $stdout = org_stdout
    end
  end
end