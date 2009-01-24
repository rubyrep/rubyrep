require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe BaseRunner do
  before(:each) do
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = BaseRunner.new
    status = runner.process_options ["--nonsense"]
    runner.options.should == nil
    status.should == 1
  end
  
  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = BaseRunner.new
    status = runner.process_options ["table"]
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should show the summary description (if usage is printed)" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      base_runner = BaseRunner.new
      base_runner.should_receive(:summary_description).
        and_return("my_summary_description")
      base_runner.process_options ["--help"]
      $stderr.string.should =~ /my_summary_description/
    ensure
      $stderr = org_stderr
    end
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    runner = BaseRunner.new
    status = runner.process_options ["--help"]
    runner.options.should == nil
    status.should == 0
  end
  
  it "process_options should set the correct options" do
    runner = BaseRunner.new
    runner.process_options ["-c", "config_path", "table_spec1", "table_spec2"]
    runner.options[:config_file].should == 'config_path'
    runner.options[:table_specs].should == ['table_spec1', 'table_spec2']
  end

  it "process_options should add runner specific options" do
    BaseRunner.any_instance_should_receive(:add_specific_options) do
      runner = BaseRunner.new
      runner.process_options ["-c", "config_path"]
    end
  end
  
  it "process_options should assign the command line specified report printer" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      
      ScanReportPrinters.register :dummy_printer_class, "-y", "--printer_y[=arg]", "description"
      
      runner = BaseRunner.new
      runner.stub!(:session)
      runner.process_options ["-c", "config_path", "--printer_y=arg_for_y", "table_spec"]
      runner.report_printer_class.should == :dummy_printer_class
      runner.report_printer_arg.should == 'arg_for_y'
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end

  it "process_options should assign the command line specified progress printer class" do
    org_printers = ScanProgressPrinters.printers
    begin
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, nil }

      printer_y_class = mock("printer_y_class")
      printer_y_class.should_receive(:arg=)

      ScanProgressPrinters.register :printer_y_key, printer_y_class, "-y", "--printer_y[=arg]", "description"

      runner = BaseRunner.new
      runner.process_options ["-c", "config_path", "-y", "arg_for_y"]
      runner.progress_printer.should == printer_y_class
    ensure
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, org_printers }
    end
  end

  it "add_specific_options should not do anything" do
    BaseRunner.new.add_specific_options nil
  end

  it "create_processor should not do anything" do
    BaseRunner.new.create_processor "dummy_left_table", "dummy_right_table"
  end

  it "prepare_table_pairs should return the provided table pairs unmodied" do
    BaseRunner.new.prepare_table_pairs(:dummy_table_pairs).
      should == :dummy_table_pairs
  end
  
  it "run should not start a scan if the command line is invalid" do
    $stderr.should_receive(:puts).any_number_of_times
    BaseRunner.any_instance_should_not_receive(:execute) {
      BaseRunner.run(["--nonsense"])
    }
  end

  it "run should start a scan if the command line is correct" do
    BaseRunner.any_instance_should_receive(:execute) {
      BaseRunner.run(["--config=path", "table"])
    }
  end

  it "report_printer should create and return the printer as specified per command line options" do
    printer_class = mock("printer class")
    printer_class.should_receive(:new).with(:dummy_session, :dummy_arg).and_return(:dummy_printer)
    runner = BaseRunner.new
    runner.stub!(:session).and_return(:dummy_session)
    runner.report_printer_class = printer_class
    runner.report_printer_arg = :dummy_arg
    runner.report_printer.should == :dummy_printer
    runner.report_printer # ensure the printer object is cached
  end
  
  it "report_printer should return the ScanSummaryReporter if no other printer was chosen" do
    runner = BaseRunner.new
    runner.stub!(:session)
    runner.report_printer.should be_an_instance_of(ScanReportPrinters::ScanSummaryReporter)
  end

  it "progress_printer should return the config file specified printer if none was give via command line" do
    runner = BaseRunner.new
    runner.options = {
      :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
      :table_specs => ["scanner_records", "extender_one_record"]
    }
    config_specified_printer_key = Session.new(standard_config).configuration.options[:scan_progress_printer]
    config_specified_printer_class = ScanProgressPrinters.
      printers[config_specified_printer_key][:printer_class]
    runner.progress_printer.should == config_specified_printer_class
  end
  
  it "signal_scanning_completion should signal completion if the scan report printer supports it" do
    printer = mock("printer")
    printer.should_receive(:scanning_finished)
    printer.should_receive(:respond_to?).with(:scanning_finished).and_return(true)
    runner = BaseRunner.new
    runner.stub!(:report_printer).and_return(printer)
    runner.signal_scanning_completion
  end
  
  it "signal_scanning_completion should not signal completion if the scan report printer doesn't supports it" do
    printer = mock("printer")
    printer.should_not_receive(:scanning_finished)
    printer.should_receive(:respond_to?).with(:scanning_finished).and_return(false)
    runner = BaseRunner.new
    runner.stub!(:report_printer).and_return(printer)
    runner.signal_scanning_completion
  end

  it "execute should process the specified tables" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      runner = BaseRunner.new
      runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records", "extender_one_record"]
      }

      # create and install a dummy processor
      processor = mock("dummy_processor")
      processor.should_receive(:run).twice.and_yield(:left, :dummy_row)

      # verify that the scanner receives the progress printer
      runner.stub!(:progress_printer).and_return(:dummy_printer_class)
      processor.should_receive(:progress_printer=).twice.with(:dummy_printer_class)

      runner.should_receive(:create_processor).twice.and_return(processor)

      # verify that the scanning_completion signal is given to scan report printer
      runner.should_receive :signal_scanning_completion

      runner.execute

      # verify that rubyrep infrastructure tables were excluded
      runner.session.configuration.excluded_table_specs.include?(/^rr_.*/).should be_true

      $stdout.string.should =~ /scanner_records.* 1\n/
      $stdout.string.should =~ /extender_one_record.* 1\n/
    ensure
      $stdout = org_stdout
    end
  end

  it "table_pairs should return the prepared table pairs" do
    runner = BaseRunner.new
    runner.options = {
      :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
      :table_specs => ['scanner_records']
    }
    runner.should_receive(:prepare_table_pairs).with([
      {:left => 'scanner_records', :right => 'scanner_records'},
    ]).and_return(:dummy_table_pairs)
    runner.table_pairs.should == :dummy_table_pairs
  end
end