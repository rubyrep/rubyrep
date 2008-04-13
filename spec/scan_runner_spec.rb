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
  
  it "get_options should correct register the options of the scan report printers" do
    org_printers = ScanRunner.report_printers
    begin
      ScanRunner.instance_eval { class_variable_set :@@report_printers, nil }
      
      # register a printer which will not be selected in the command line options
      printer_x = mock("printer_x")
      printer_x.should_not_receive :new
      ScanRunner.register_printer printer_x, "-x", "--printer_x"
      
      # register a printer that will be selected in the command line options
      printer_y = mock("printer_y")
      printer_y.should_receive(:new).and_return(:printer_y_instance)
      
      ScanRunner.register_printer printer_y, "-y", "--printer_y", "[=arg_for_y]"
      
      scan_runner = ScanRunner.new
      scan_runner.get_options ["-c", "config_path", "-y", "arg_for_y", "table_spec"]
      scan_runner.active_printer.should == :printer_y_instance
    ensure
      ScanRunner.instance_eval { class_variable_set :@@report_printers, org_printers }
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
  
  it "report_printers should an empty array if there are no registered printers" do
    org_printers = ScanRunner.report_printers
    begin
      ScanRunner.instance_eval { class_variable_set :@@report_printers, nil }
      ScanRunner.report_printers.should == []
    ensure
      ScanRunner.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
  
  it "register_printer should store the provided printers, report_printer should return them" do
    org_printers = ScanRunner.report_printers
    begin
      ScanRunner.instance_eval { class_variable_set :@@report_printers, nil }
      ScanRunner.register_printer :dummy_printer_class, "-d", "--dummy"
      ScanRunner.register_printer :another_printer_class, "-t"
      ScanRunner.report_printers.should == [
        { :printer_class => :dummy_printer_class,
          :opts => ["-d", "--dummy"]
        },
        { :printer_class => :another_printer_class,
          :opts => ["-t"]
        }
      ]
    ensure
      ScanRunner.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
  
  it "active_printer should return the printer as assigned by active_printer=" do
    scan_runner = ScanRunner.new
    scan_runner.active_printer= :dummy
    scan_runner.active_printer.should == :dummy
  end
  
  it "active_printer should return the ScanSummaryReporter if no other printer was chosen" do
    ScanRunner.new.active_printer.should be_an_instance_of(ScanSummaryReporter)
  end
  
  it "table_scan_class should return TableScan for non-proxied sessions" do
    session = mock("session")
    session.should_receive(:proxied?).and_return(false)
    scan_runner = ScanRunner.new
    scan_runner.table_scan_class(session).should == DirectTableScan
  end
  
  it "table_scan_class should return ProxiedTableScan for proxied sessions" do
    session = mock("session")
    session.should_receive(:proxied?).and_return(true)
    scan_runner = ScanRunner.new
    scan_runner.table_scan_class(session).should == ProxiedTableScan
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
      scan_runner.active_printer = ScanSummaryReporter.new("totals_only")
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