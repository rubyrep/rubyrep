require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe SyncRunner do
  before(:each) do
  end

  it "rrsync.rb should call ScanRunner#run" do
    SyncRunner.should_receive(:run).with(ARGV).and_return(0)
    Kernel.any_instance_should_receive(:exit) {
      load File.dirname(__FILE__) + '/../bin/rrsync.rb'
    }
  end
  
  it "prepare_table_pairs should sort the tables if that was enabled" do
    session = Session.new standard_config
    session.should_receive(:sort_table_pairs).
      with(:dummy_table_pairs).
      and_return(:sorted_dummy_table_pairs)

    sync_runner = SyncRunner.new
    sync_runner.stub!(:session).and_return(session)
    sync_runner.should_receive(:table_ordering?).and_return true

    sync_runner.prepare_table_pairs(:dummy_table_pairs).should == :sorted_dummy_table_pairs
  end

  it "prepare_table_pairs should not sort the tables if that was disabled" do
    sync_runner = SyncRunner.new
    sync_runner.should_receive(:table_ordering?).and_return false
    sync_runner.prepare_table_pairs(:dummy).should == :dummy
  end

  it "execute should sync the specified tables" do
    org_stdout = $stdout
    session = nil

    # This is necessary to avoid the cached RubyRep configurations from getting
    # overwritten by the sync run
    old_config, Initializer.configuration = Initializer.configuration, Configuration.new

    session = Session.new(standard_config)
    session.left.begin_db_transaction
    session.right.begin_db_transaction

    $stdout = StringIO.new
    begin
      sync_runner = SyncRunner.new
      sync_runner.report_printer = ScanReportPrinters::ScanSummaryReporter.new(nil)
      sync_runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records"]
      }

      sync_runner.execute

      $stdout.string.should =~
        /scanner_records .* 5\n/

      left_records = session.left.connection.select_all("select * from scanner_records order by id")
      right_records = session.right.connection.select_all("select * from scanner_records order by id")
      left_records.should == right_records
    ensure
      $stdout = org_stdout
      Initializer.configuration = old_config if old_config
      if session
        session.left.rollback_db_transaction
        session.right.rollback_db_transaction
      end
    end
  end

  it "create_processor should create the TableSync instance" do
    TableSync.should_receive(:new).
      with(:dummy_session, "left_table", "right_table").
      and_return(:dummy_table_sync)
    sync_runner = SyncRunner.new
    sync_runner.should_receive(:session).and_return(:dummy_session)
    sync_runner.create_processor("left_table", "right_table").
      should == :dummy_table_sync
  end

  it "summary_description should return a description" do
    SyncRunner.new.summary_description.should be_an_instance_of(String)
  end

  it "table_ordering? should only return true if it is enabled via configuration file and not disabled via command line" do
    enabled_config = mock("enabled_configuration")
    enabled_config.stub!(:options).and_return(:table_ordering => true)
    enabled_session = mock("enabled session")
    enabled_session.stub!(:configuration).and_return(enabled_config)

    disabled_config = mock("disabled_configuration")
    disabled_config.stub!(:options).and_return(:table_ordering => false)
    disabled_session = mock("disabled session")
    disabled_session.stub!(:configuration).and_return(disabled_config)

    sync_runner = SyncRunner.new

    sync_runner.stub!(:session).and_return(disabled_session)
    sync_runner.stub!(:options).and_return({})
    sync_runner.table_ordering?.should be_false
    sync_runner.stub!(:options).and_return(:no_table_ordering => true)
    sync_runner.table_ordering?.should be_false

    sync_runner.stub!(:session).and_return(enabled_session)
    sync_runner.stub!(:options).and_return({})
    sync_runner.table_ordering?.should be_true
    sync_runner.stub!(:options).and_return(:no_table_ordering => true)
    sync_runner.table_ordering?.should be_false
  end

  it "add_specific_options should add '--no-table-ordering' option" do
    runner = SyncRunner.new
    runner.options = {}

    opts = mock("dummy option parser")
    opts.should_receive(:on).with("--no-table-ordering", an_instance_of(String)).
      and_yield
    runner.add_specific_options opts

    runner.options[:no_table_ordering].should be_true
  end
end