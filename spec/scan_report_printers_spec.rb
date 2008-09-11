require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters do
  before(:each) do
  end

  it "report_printers should an empty array if there are no registered printers" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      ScanReportPrinters.printers.should == []
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
  
  it "register_printer should store the provided printers, report_printer should return them" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      ScanReportPrinters.register :dummy_printer_class, "-d", "--dummy"
      ScanReportPrinters.register :another_printer_class, "-t"
      ScanReportPrinters.printers.should == [
        { :printer_class => :dummy_printer_class,
          :opts => ["-d", "--dummy"]
        },
        { :printer_class => :another_printer_class,
          :opts => ["-t"]
        }
      ]
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
end