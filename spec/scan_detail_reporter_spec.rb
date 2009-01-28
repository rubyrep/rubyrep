require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters::ScanDetailReporter do
  before(:each) do
    $stdout.should_receive(:puts).any_number_of_times
  end

  it "should register itself with ScanRunner" do
    RR::ScanReportPrinters.printers.any? do |printer|
      printer[:printer_class] == ScanReportPrinters::ScanDetailReporter
    end.should be_true
  end
  
  it "initialize should store the provided session" do
    ScanReportPrinters::ScanDetailReporter.new(:dummy_session, nil).session.should == :dummy_session
  end
  
  it "scan should print the summary and details of the differences" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanDetailReporter.new(nil, nil)
      
      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}
      
      reporter.scan('left_table', 'right_table') do 
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      $stdout.string.should =~ /left_table \/ right_table [\.\s]*3\n/
      $stdout.string.should =~ /:conflict.*dummy_row/
      $stdout.string.should =~ /:left.*dummy_row/
      $stdout.string.should =~ /:right.*dummy_row/
    ensure 
      $stdout = org_stdout
    end
  end
end