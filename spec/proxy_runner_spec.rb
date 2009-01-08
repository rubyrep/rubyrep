require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyRunner do
  before(:each) do
    DRb.stub!(:start_service)
    DRb.thread.stub!(:join)
    $stderr.stub!(:puts)
  end

  it "get_options should return options as nil and status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    options, status = ProxyRunner.new.get_options ["--nonsense"]
    options.should == nil
    status.should == 1
  end
  
  it "get_options should return options as nil and status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    options, status = ProxyRunner.new.get_options ["--help"]
    options.should == nil
    status.should == 0
  end
  
  it "get_options should return the default options if none were given on the command line" do
    options, status = ProxyRunner.new.get_options []
    options.should == ProxyRunner::DEFAULT_OPTIONS
    status.should == 0
  end
  
  it "get_options should return :host and :port options as per given command line" do
    options, status = ProxyRunner.new.get_options ["--host", "127.0.0.1", "--port", "1234"]
    options.should == {:host => '127.0.0.1', :port => 1234}
    status.should == 0
  end

  it "construct_url should create the correct druby URL" do
    ProxyRunner.new.build_url(:host => '127.0.0.1', :port => '1234').should == "druby://127.0.0.1:1234"
  end
  
  it "start_server should create a DatabaseProxy and start the DRB server" do
    DatabaseProxy.should_receive(:new)
    DRb.should_receive(:start_service,"druby://127.0.0.1:1234")
    DRb.stub!(:thread).and_return(Object.new)
    DRb.thread.should_receive(:join)
    ProxyRunner.new.start_server("druby://127.0.0.1:1234")
  end
  
  it "run should not start a server if the command line is invalid" do
    DRb.should_not_receive(:start_service)
    DRb.stub!(:thread).and_return(Object.new)
    DRb.thread.should_not_receive(:join)
    ProxyRunner.run("--nonsense")    
  end
  
  it "run should start a server if the command line is correct" do
    DRb.should_receive(:start_service)
    DRb.stub!(:thread).and_return(Object.new)
    DRb.thread.should_receive(:join)
    ProxyRunner.run(["--port=1234"])    
  end
  
  it "should register itself with CommandRunner" do
    CommandRunner.commands['proxy'][:command].should == ProxyRunner
    CommandRunner.commands['proxy'][:description].should be_an_instance_of(String)
  end
end