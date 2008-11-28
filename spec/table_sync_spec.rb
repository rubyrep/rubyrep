require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSync do
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
    config = deep_copy(standard_config)
    config.options[:committer] = :never_commit
    config.options[:logged_sync_events] = [:all_conflicts]
    session = Session.new(config)
    begin
      sync = TableSync.new(session, 'scanner_records')
      sync.run

      # Verify that sync events are logged
      row = session.left.select_one("select * from rr_event_log where change_key = '2' order by id")
      row['change_table'].should == 'scanner_records'
      row['diff_type'].should == 'conflict'
      row['description'].should == 'update_right'

      # verify that the table was synchronized
      left_records = session.left.select_all("select * from scanner_records order by id")
      right_records = session.right.select_all("select * from scanner_records order by id")
      left_records.should == right_records
    ensure
      Committers::NeverCommitter.rollback_current_session
      session.left.execute "delete from rr_event_log"
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
