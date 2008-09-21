require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSync do
  before(:each) do
    @old_syncers = TableSync.send :class_variable_get, :@@syncers rescue nil
    TableSync.send :class_variable_set, :@@syncers, nil
  end

  after(:each) do
    TableSync.send :class_variable_set, :@@syncers, @old_syncers
  end
  
  it "syncers should return empty hash if empty" do
    TableSync.syncers.should == {}
  end
  
  it "register_syncer should register, syncer return the registerred syncers" do
    TableSync.register_syncer :key1 => :dummy_syncer1
    TableSync.register_syncer :key2 => :dummy_syncer2
    TableSync.syncers.should == {:key1 => :dummy_syncer1, :key2 => :dummy_syncer2}
  end
  
  it "sync_options should return the correct table specific sync options" do
    config = deep_copy(standard_config)
    old_table_specific_options = config.tables_with_options
    begin
      config.options = {:syncer => :bla}
      config.add_tables 'scanner_records', {:syncer => :blub}
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
