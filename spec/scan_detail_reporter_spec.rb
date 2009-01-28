require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters::ScanDetailReporter do
  before(:each) do
    Initializer.configuration = standard_config
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
  
  it "scan should print the summary and the dump of the differences if mode = 'full'" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanDetailReporter.new(nil, 'full')
      
      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}
      
      reporter.scan('left_table', 'right_table') do 
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      
      # verify summary
      $stdout.string.should =~ /left_table \/ right_table [\.\s]*3\n/

      # verify dump
      io = StringIO.new($stdout.string.gsub(/^.*left_table.*$/, ''))
      dump_objects = []
      YAML.load_documents(io) do |yl|
        dump_objects << yl
      end
      dump_objects.should == [
        {:conflict=>:dummy_row},
        {:left=>:dummy_row},
        {:right=>:dummy_row}
      ]
    ensure 
      $stdout = org_stdout
    end
  end

  it "scan should print the summary and the keys of the differences if mode = 'keys'" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      session = Session.new
      reporter = ScanReportPrinters::ScanDetailReporter.new(session, 'keys')

      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}

      reporter.scan('scanner_records', 'scanner_records') do
        reporter.report_difference :conflict, [{'id' => 1, 'name' => 'bla'}, {'id' => 1, 'name' => 'blub'}]
        reporter.report_difference :left, {'id' => 2, 'name' => 'bla'}
        reporter.report_difference :right, {'id' => 3, 'name' => 'blub'}
      end

      io = StringIO.new($stdout.string.gsub(/^.*scanner_records.*$/, ''))
      dump_objects = []
      YAML.load_documents(io) do |yl|
        dump_objects << yl
      end
      dump_objects.should == [
        {:conflict=>{"id"=>1}},
        {:left=>{"id"=>2}},
        {:right=>{"id"=>3}}
      ]
    ensure
      $stdout = org_stdout
    end
  end

  it "scan should print the summary and the differing columns of the differences if mode = 'diff'" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      session = Session.new
      reporter = ScanReportPrinters::ScanDetailReporter.new(session, 'diff')

      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}

      reporter.scan('scanner_records', 'scanner_records') do
        reporter.report_difference :conflict, [
          {'id' => 1, 'name' => 'bla', 'age' => 20},
          {'id' => 1, 'name' => 'blub', 'age' => 20}
        ]
        reporter.report_difference :left, {'id' => 2, 'name' => 'bla'}
        reporter.report_difference :right, {'id' => 3, 'name' => 'blub'}
      end

      io = StringIO.new($stdout.string.gsub(/^.*scanner_records.*$/, ''))
      dump_objects = []
      YAML.load_documents(io) do |yl|
        dump_objects << yl
      end
      dump_objects.should == [
        {:conflict=>[{'id' => 1, 'name' => 'bla'}, {'id' => 1, 'name' => 'blub'}]},
        {:left=>{"name"=>"bla", "id"=>2}},
        {:right=>{"name"=>"blub", "id"=>3}}
      ]
    ensure
      $stdout = org_stdout
    end
  end
end