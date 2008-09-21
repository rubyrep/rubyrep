require File.dirname(__FILE__) + '/spec_helper.rb'

include RR
include ScanProgressPrinters

describe ProgressBar do
  before(:each) do
    @org_stdout = $stdout
    $stdout = StringIO.new
    @old_arg = ProgressBar.arg
  end

  after(:each) do
    ProgressBar.arg = @old_arg
    $stdout = @org_stdout
  end

  it "arg should store the command line argument and max_markers return the correct marker number" do
    ProgressBar.arg = nil
    ProgressBar.new(100, 'bla', 'blub').max_markers.should == ProgressBar::MAX_MARKERS
    ProgressBar.arg = "2"
    ProgressBar.new(100, 'bla', 'blub').max_markers.should == 2
  end

  it "should register itself with ScanRunner" do
    RR::ScanProgressPrinters.printers[:progress_bar][:printer_class].
      should == ProgressBar
  end

  it "step should print the correct progress" do
    bar = ProgressBar.new(1000, 'bla', 'blub')
    bar.step 200
    bar.step 300
    $stdout.string.should =~ /^\.{20}$/
    bar.step 500
    $stdout.string.should =~ /^\.{40}\s*$/
  end

end
