require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanProgressPrinters do
  before(:each) do
  end

  it "report_printers should an empty hash if there are no registered printers" do
    org_printers = ScanProgressPrinters.printers
    begin
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, nil }
      ScanProgressPrinters.printers.should == {}
    ensure
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, org_printers }
    end
  end
  
  it "register_printer should store the provided printers, printers should return them" do
    org_printers = ScanProgressPrinters.printers
    begin
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, nil }
      ScanProgressPrinters.register :dummy_printer_id, :dummy_printer_class, "-d", "--dummy"
      ScanProgressPrinters.register :another_printer_id, :another_printer_class, "-t"
      ScanProgressPrinters.printers.should == {
        :dummy_printer_id => {
          :printer_class => :dummy_printer_class,
          :opts => ["-d", "--dummy"]
        },
        :another_printer_id => {
          :printer_class => :another_printer_class,
          :opts => ["-t"]
        }
      }
    ensure
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, org_printers }
    end
  end

  it "on_printer_selection should create and yield the correct printer" do
    org_printers = ScanProgressPrinters.printers
    begin
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, nil }

      # register a printer which will not be selected in the command line options
      printer_x_class = mock("printer_x")
      printer_x_class.should_not_receive :arg=
      ScanProgressPrinters.register :printer_x_id, printer_x_class, "-x", "--printer_x"

      # register a printer that will be selected in the command line options
      printer_y_class = mock("printer_y")
      printer_y_class.should_receive(:arg=).with("dummy_arg")

      ScanProgressPrinters.register :printer_y_id, printer_y_class, "-y", "--printer_y[=arg]", "description"

      selected_printer = nil
      parser = OptionParser.new
      ScanProgressPrinters.on_printer_selection(parser) do |printer|
        selected_printer = printer
      end
      parser.parse!(["--printer_y=dummy_arg"])

      selected_printer.should == printer_y_class
    ensure
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, org_printers }
    end
  end
end