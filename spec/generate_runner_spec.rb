require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe GenerateRunner do
  before(:each) do
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['generate'][:command].should == GenerateRunner
    CommandRunner.commands['generate'][:description].should be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = GenerateRunner.new
    status = runner.process_options ["--nonsense"]
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 1 if file name is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = GenerateRunner.new
    status = runner.process_options []
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    runner = GenerateRunner.new
    status = runner.process_options ["--help"]
    runner.options.should == nil
    status.should == 0
  end

  it "process_options should set the correct options" do
    runner = GenerateRunner.new
    runner.process_options ["my_file_name"]
    runner.options[:file_name].should == 'my_file_name'
  end

  it "run should not start the generate command if the command line is invalid" do
    $stderr.should_receive(:puts).any_number_of_times
    GenerateRunner.any_instance_should_not_receive(:execute) {
      GenerateRunner.run(["--nonsense"])
    }
  end

  it "run should start an uninstall if the command line is correct" do
    GenerateRunner.any_instance_should_receive(:execute) {
      GenerateRunner.run(["my_file_name"])
    }
  end

  it "execute should refuse to overwrite an existing file" do
    begin
      File.open("my_config_template", 'w') do |f|
        f.write 'bla'
      end
      runner = GenerateRunner.new
      runner.options = {:file_name => 'my_config_template'}
      lambda {runner.execute}.should raise_error(/refuse/)
    ensure
      File.delete('my_config_template') rescue nil
    end
  end

  it "execute should create the configuration template under the specified name" do
    begin
      runner = GenerateRunner.new
      runner.options = {:file_name => 'my_config_template'}
      runner.execute
      File.exists?('my_config_template').should be_true
    ensure
      File.delete 'my_config_template' rescue nil
    end
  end

end