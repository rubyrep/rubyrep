require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSync do
  it "syncer_class should return the correct syncer as per :syncer option, if both :syncer and :replicator is configured" do
    config = deep_copy(standard_config)
    config.options[:syncer] = :two_way
    config.options[:replicator] = :key2
    session = Session.new(config)
    TableSync.new(session, 'scanner_records').syncer_class.should == Syncers::TwoWaySyncer
  end
  
  it "syncer_class should return the correct syncer as per :replicator option if no :syncer option is provided" do
    config = deep_copy(standard_config)
    config.options[:replicator] = :two_way
    config.options.delete :syncer
    session = Session.new(config)
    TableSync.new(session, 'scanner_records').syncer_class.should == Syncers::TwoWaySyncer
  end

  it "sync_options should return the correct table specific sync options" do
    config = deep_copy(standard_config)
    old_table_specific_options = config.tables_with_options
    begin
      config.options = {:syncer => :bla}
      config.include_tables 'scanner_records', {:syncer => :blub}
      TableSync.new(Session.new(config), 'scanner_records').sync_options[:syncer] \
        .should == :blub
    ensure
      config.instance_eval {@tables_with_options = old_table_specific_options}
    end
  end

  it "run should synchronize the databases" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:delete] = true

      session = Session.new(config)
      sync = TableSync.new(session, 'scanner_records')
      sync.run

      left_records = session.left.connection.select_all("select * from scanner_records order by id")
      right_records = session.right.connection.select_all("select * from scanner_records order by id")

      left_records.should == right_records
    ensure
      Committers::NeverCommitter.rollback_current_session
    end
  end

#  it "run should hand it's progress reporter to the scan class" do
#    begin
#      config = deep_copy(standard_config)
#      config.options[:committer] = :never_commit
#      config.options[:delete] = true
#
#      session = Session.new(config)
#      sync = TableSync.new(session, 'scanner_records')
#
#      TableScan.any_instance_should_receive(:progress_reporter=) do
#        sync.run
#      end
#    ensure
#      Committers::NeverCommitter.rollback_current_session
#    end
#  end
end  
