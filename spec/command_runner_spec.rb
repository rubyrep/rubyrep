require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe CommandRunner do
  before(:each) do
    @org_commands = CommandRunner.commands
    CommandRunner.instance_variable_set :@commands, nil
  end

  after(:each) do
    CommandRunner.instance_variable_set :@commands, @org_commands
  end

  it "show_version should print the version string" do
    $stdout.should_receive(:puts).with(/rubyrep version ([0-9]+\.){2}[0-9]+/)
    CommandRunner.show_version
  end

  it "register should register commands, commands should return it" do
    CommandRunner.register :bla => :bla_command
    CommandRunner.register :blub => :blub_command
    CommandRunner.commands.should == {
      :bla => :bla_command,
      :blub => :blub_command
    }
  end

  it "show_help should print a short help" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      CommandRunner.register 'c1' => {:description => 'desc 1'}, 'c2' => {:description => 'desc 2'}
      CommandRunner.show_help
      $stderr.string.should =~ /Usage/
      $stderr.string.should =~ /c1.*desc 1\n/
      $stderr.string.should =~ /c2.*desc 2\n/
    ensure
      $stderr = org_stderr
    end
  end

  it "run should print help if no command line parameters are given" do
    CommandRunner.should_receive(:show_help)
    CommandRunner.run([]).should == 1
  end

  it "run should print help if --help or help without further params is given" do
    CommandRunner.should_receive(:show_help).twice
    CommandRunner.run(['--help']).should == 0
    CommandRunner.run(['help']).should == 0
  end

  it "run should print version if --version is given" do
    CommandRunner.should_receive(:show_version)
    CommandRunner.run(['--version'])
  end

  it "run should call the specified command with the specified params" do
    c = mock('dummy_command')
    c.should_receive(:run).with(['param1', 'param2'])
    CommandRunner.register 'dummy_command' => {:command => c}
    CommandRunner.run(['dummy_command', 'param1', 'param2'])
  end

  it "run should print help if unknown command is given" do
    CommandRunner.should_receive(:show_help)
    CommandRunner.run('non-existing-command').should == 1
  end

  it "rubyrep.rb should call CommandRunner#run" do
    CommandRunner.should_receive(:run).with(ARGV).and_return(0)
    Kernel.any_instance_should_receive(:exit) {
      load File.dirname(__FILE__) + '/../bin/rubyrep'
    }
  end
end

describe HelpRunner do
  it "should register itself" do
    CommandRunner.commands['help'][:command].should == HelpRunner
    CommandRunner.commands['help'][:description].should be_an_instance_of(String)
  end

  it "run should call help for the specified command" do
    CommandRunner.should_receive(:run).with(['dummy_command', '--help'])
    HelpRunner.run(['dummy_command'])
  end
end