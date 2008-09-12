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
  
  it "get_options should return options as nil and status as 1 if config option is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    options, status = ScanRunner.new.get_options ["table"]
    options.should == nil
    status.should == 1
  end
  
  it "get_options should return options as nil and status as 1 if no table_spec is given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    options, status = ScanRunner.new.get_options ["--config=path"]
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
  
  it "get_options should return the correct options" do
    options, _ = ScanRunner.new.get_options ["-c", "config_path", "table_spec1", "table_spec2"]
    options[:config_file].should == 'config_path'
    options[:table_specs].should == ['table_spec1', 'table_spec2']
  end
  
  it "get_options should assign the command line specified printer" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      
      printer_y = mock("printer_y")
      printer_y.should_receive(:new).and_return(:printer_y_instance)
      
      ScanReportPrinters.register printer_y, "-y", "--printer_y[=arg]", "description"
      
      scan_runner = ScanRunner.new
      scan_runner.get_options ["-c", "config_path", "-y", "arg_for_y", "table_spec"]
      scan_runner.active_printer.should == :printer_y_instance
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
  
  it "run should not start a scan if the command line is invalid" do
    $stderr.should_receive(:puts).any_number_of_times
    ScanRunner.any_instance_should_not_receive(:scan) {
      ScanRunner.run(["--nonsense"])
    }
  end

  it "run should start a scan if the command line is correct" do
    ScanRunner.any_instance_should_receive(:scan) {
      ScanRunner.run(["--config=path", "table"])
    }
  end
  
  it "rrscan.rb should call ScanRunner#run" do
    ScanRunner.should_receive(:run).with(ARGV).and_return(0)
    Kernel.any_instance_should_receive(:exit) {
      load File.dirname(__FILE__) + '/../bin/rrscan.rb'
    }
  end
  
  it "active_printer should return the printer as assigned by active_printer=" do
    scan_runner = ScanRunner.new
    scan_runner.active_printer= :dummy
    scan_runner.active_printer.should == :dummy
  end
  
  it "active_printer should return the ScanSummaryReporter if no other printer was chosen" do
    ScanRunner.new.active_printer.should be_an_instance_of(ScanReportPrinters::ScanSummaryReporter)
  end
  
  it "signal_scanning_completion should signal completion if the scan report printer supports it" do
    printer = mock("printer")
    printer.should_receive(:scanning_finished)
    printer.should_receive(:respond_to?).with(:scanning_finished).and_return(true)
    scan_runner = ScanRunner.new
    scan_runner.active_printer = printer
    scan_runner.signal_scanning_completion
  end
  
  it "signal_scanning_completion should not signal completion if the scan report printer doesn't supports it" do
    printer = mock("printer")
    printer.should_not_receive(:scanning_finished)
    printer.should_receive(:respond_to?).with(:scanning_finished).and_return(false)
    scan_runner = ScanRunner.new
    scan_runner.active_printer = printer
    scan_runner.signal_scanning_completion
  end
  
  it "scan should scan the specified tables" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      scan_runner = ScanRunner.new
      scan_runner.active_printer = ScanReportPrinters::ScanSummaryReporter.new(nil)
      options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records", "extender_one_record"]
      }
      
      # verify that the scanning_completion signal is given to scan report printer
      scan_runner.should_receive :signal_scanning_completion
      
      scan_runner.scan options
      
      $stdout.string.should == 
        "scanner_records / scanner_records 5\n" +
        "extender_one_record / extender_one_record 0\n"
    ensure 
      $stdout = org_stdout
    end
  end
end