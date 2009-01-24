require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters::ScanSummaryReporter do
  before(:each) do
    $stdout.should_receive(:puts).any_number_of_times
  end

  it "should register itself with ScanRunner" do
    RR::ScanReportPrinters.printers.any? do |printer|
      printer[:printer_class] == ScanReportPrinters::ScanSummaryReporter
    end.should be_true
  end
  
  it "initialize should detect if the detailed number of differnces should be counted" do
    ScanReportPrinters::ScanSummaryReporter.new(nil, nil).only_totals.should be_true
    ScanReportPrinters::ScanSummaryReporter.new(nil, "bla").only_totals.should be_true
    ScanReportPrinters::ScanSummaryReporter.new(nil, "detailed").only_totals.should be_false
  end
  
  it "scan should count differences correctly in totals mode" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanSummaryReporter.new(nil, nil)
      
      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}
      
      reporter.scan('left_table', 'right_table') do 
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      $stdout.string.should =~ /left_table \/ right_table [\.\s]*3\n/
    ensure 
      $stdout = org_stdout
    end
  end

  it "scan should count differences correctly in detailed mode" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanSummaryReporter.new(nil, "detailed")
      
      reporter.scan('left_table', 'left_table') do
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
        reporter.report_difference :right, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      $stdout.string.should =~ /left_table\s+1\s+2\s+3\n/
    ensure 
      $stdout = org_stdout
    end
  end
end