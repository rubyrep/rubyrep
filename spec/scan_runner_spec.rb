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
    $stdout.should_receive(:puts)
    ScanRunner.new.active_printer.should be_an_instance_of(ScanSummaryReporter)
  end
end