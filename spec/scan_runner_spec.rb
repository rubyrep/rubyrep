require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanRunner do
  before(:each) do
  end

  it "rrscan.rb should call ScanRunner#run" do
    ScanRunner.should_receive(:run).with(ARGV).and_return(0)
    Kernel.any_instance_should_receive(:exit) {
      load File.dirname(__FILE__) + '/../bin/rrscan.rb'
    }
  end
  
  it "execute should scan the specified tables" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      scan_runner = ScanRunner.new
      scan_runner.active_printer = ScanReportPrinters::ScanSummaryReporter.new(nil)
      scan_runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records", "extender_one_record"]
      }

      scan_runner.execute
      
      $stdout.string.should == 
        "scanner_records / scanner_records 5\n" +
        "extender_one_record / extender_one_record 0\n"
    ensure 
      $stdout = org_stdout
    end
  end

  it "create_processor should create the correct table scanner" do
    scan_runner = ScanRunner.new
    dummy_scan_class = mock("scan class")
    dummy_scan_class.should_receive(:new).
      with(:dummy_session, "left_table", "right_table").
      and_return(:dummy_table_scanner)
    TableScanHelper.should_receive(:scan_class).with(:dummy_session).
      and_return(dummy_scan_class)
    scan_runner.should_receive(:session).any_number_of_times.and_return(:dummy_session)
    scan_runner.create_processor("left_table", "right_table").
      should == :dummy_table_scanner
  end

  it "summary_description should return a description" do
    ScanRunner.new.summary_description.should be_an_instance_of(String)
  end
end