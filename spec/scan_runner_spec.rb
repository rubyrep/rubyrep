require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanRunner do
  before(:each) do
  end

  it "get_options should return options as nil and status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    options, status = ScanRunner.new.get_options ["--nonsense"]
    options.should == nil
    status.should == 1
  end
  
  it "get_options should return options as nil and status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    options, status = ScanRunner.new.get_options ["--help"]
    options.should == nil
    status.should == 0
  end
  
  it "run should not start a server if the command line is invalid"

  it "run should start a server if the command line is correct" do
    mock_method(ScanRunner, :scan) {ScanRunner.run([""])}
  end
  
  it "rrscan.rb should call ScanRunner#run" do
    ScanRunner.should_receive(:run).with(ARGV).and_return(0)
    mock_method(Kernel, :exit) {load File.dirname(__FILE__) + '/../bin/rrscan.rb'}
  end
end