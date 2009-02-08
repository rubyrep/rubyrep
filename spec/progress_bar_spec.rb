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
    ProgressBar.new(100, Session.new(standard_config), 'bla', 'blub').max_markers.should == ProgressBar::MAX_MARKERS
    ProgressBar.arg = "2"
    ProgressBar.new(100, Session.new(standard_config), 'bla', 'blub').max_markers.should == 2
  end

  it "step should use ANSI codes if options :use_ansi is set" do
    session = Session.new(deep_copy(standard_config))
    session.configuration.options[:use_ansi] = true
    bar = ProgressBar.new(10, session, 'bla', 'blub')
    bar.step 1
    bar.step 1
    $stdout.string.should =~ Regexp.new(Regexp.escape("\e[1"))
  end

  it "step should not use ANSI codes if options :use_ansi is not true" do
    session = Session.new(deep_copy(standard_config))
    session.configuration.options[:use_ansi] = false
    bar = ProgressBar.new(10, session, 'bla', 'blub')
    bar.step 1
    bar.step 1
    $stdout.string.should_not =~ Regexp.new(Regexp.escape("\e[1"))
  end

  it "should register itself with ScanRunner" do
    RR::ScanProgressPrinters.printers[:progress_bar][:printer_class].
      should == ProgressBar
  end

  it "step should print the correct progress" do
    bar = ProgressBar.new(1000, Session.new(standard_config), 'bla', 'blub')
    bar.step 200
    bar.step 300
    $stdout.string.count('.').should == ProgressBar::MAX_MARKERS / 2
    bar.step 500
    $stdout.string.count('.').should == ProgressBar::MAX_MARKERS
  end

  it "step should work around weird floating point rounding issues" do
    bar = ProgressBar.new(56, Session.new(standard_config), 'bla', 'blub')
    (1..56).each {bar.step 1}
    $stdout.string.count('.').should == ProgressBar::MAX_MARKERS
  end

  it "if max_steps is 0, any step call should go to 100% progress" do
    config = deep_copy(standard_config)
    config.options[:use_ansi] = true
    bar = ProgressBar.new(0, Session.new(config), 'bla', 'blub')
    bar.step
    $stdout.string.count('.').should == ProgressBar::MAX_MARKERS
    $stdout.string.should =~ /100%/

    # Ensure we don't go over 100%
    $stdout = StringIO.new
    bar.step
    $stdout.string.should == ''
  end

end
