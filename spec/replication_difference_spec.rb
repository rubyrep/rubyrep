require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationDifference do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should store the loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new session
    diff = ReplicationDifference.new loaders
    diff.loaders.should == loaders
  end

  it "loaded? should return true if a difference was loaded" do
    diff = ReplicationDifference.new LoggedChangeLoaders.new(Session.new)
    diff.should_not be_loaded
    diff.loaded = true
    diff.should be_loaded
  end

  it "load should leave the instance unloaded if no changes are available" do
    diff = ReplicationDifference.new LoggedChangeLoaders.new(Session.new)
    diff.load
    diff.should_not be_loaded
  end

  it "load should load left differences successfully" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      diff.should be_loaded
      diff.type.should == :left
      diff.changes[:left].key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load should load right differences successfully" do
    session = Session.new
    session.right.begin_db_transaction
    begin
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      diff.should be_loaded
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}
    ensure
      session.right.rollback_db_transaction
    end
  end

  it "load should load conflict differences successfully" do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables "table_with_manual_key, extender_without_key"

    session = Session.new config
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'dummy_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'table_with_manual_key',
        'change_key' => 'id|1',
        'change_new_key' => 'id|1',
        'change_type' => 'U',
        'change_time' => 5.seconds.from_now
      }
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_without_key',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => 5.seconds.ago
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      diff.should be_loaded
      diff.type.should == :conflict
      diff.changes[:left].type.should == :update
      diff.changes[:left].table.should == 'table_with_manual_key'
      diff.changes[:left].key.should == {'id' => '1'}
      diff.changes[:right].type.should == :delete
      diff.changes[:right].table.should == 'extender_without_key'
      diff.changes[:right].key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
      session.right.rollback_db_transaction
    end
  end

  it "amend should amend the replication difference with new found changes" do
    session = Session.new
    session.left.begin_db_transaction
    session.right.begin_db_transaction
    begin
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      diff.should be_loaded
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}

      # if there are no changes, the diff should still be the same
      diff.amend
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}

      # should recognize new changes
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      diff.amend
      diff.type.should == :conflict
      diff.changes[:left].key.should == {'id' => '1'}
      diff.changes[:right].key.should == {'id' => '1'}
    ensure
      session.right.rollback_db_transaction
      session.left.rollback_db_transaction
    end
  end

  it "to_yaml should blank out session" do
    diff = ReplicationDifference.new :dummy_session
    diff.to_yaml.should_not =~ /session/
  end
end
