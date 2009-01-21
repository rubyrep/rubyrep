require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TriggerModeSwitcher do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should save the session and initialize triggers hash" do
    session = Session.new
    switcher = TriggerModeSwitcher.new(session)
    switcher.session.should == session
  end

  it "exclude_rr_activity should switch the trigger for the given table" do
    switcher = TriggerModeSwitcher.new(Session.new)

    switcher.should_receive(:switch_trigger_mode).with(:right, 'right1', true).once
    switcher.exclude_rr_activity(:right, 'right1')

    # Verify that for a given table, the trigger is not modified multiple times
    switcher.exclude_rr_activity(:right, 'right1')
  end

  it "restore_triggers should restore the triggers" do
    switcher = TriggerModeSwitcher.new(Session.new)

    switcher.stub!(:switch_trigger_mode)
    switcher.exclude_rr_activity :left, 'left1'
    switcher.should_receive(:switch_trigger_mode).with(:left, 'left1', false).once
    switcher.restore_triggers
    switcher.restore_triggers # ensure the restore is only done once
  end

  it "switch_trigger_mode should switch the exclude_rr_activity mode as specified" do
    session = nil
    initializer = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      initializer.create_trigger(:left, 'trigger_test')

      switcher = TriggerModeSwitcher.new session
      switcher.switch_trigger_mode :left, 'trigger_test', true
      
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'blub'
      }
      session.left.execute "insert into rr_running_flags values(1)"
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 3,
        'name' => 'bla'
      }

      rows = session.left.select_all("select * from rr_pending_changes order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
        'change_table' => 'trigger_test',
        'change_key' => 'first_id|1|second_id|2',
        'change_new_key' => nil,
        'change_type' => 'I'
      }]
    ensure
      initializer.drop_trigger :left, 'trigger_test' if initializer
      if session
        session.left.execute 'delete from rr_running_flags'
        session.left.execute 'delete from trigger_test'
        session.left.execute 'delete from rr_pending_changes'
      end
    end
  end

  it "switch_trigger_mode should not switch the trigger mode if the table has no trigger" do
    session = Session.new
    switcher = TriggerModeSwitcher.new session
    session.left.should_not_receive(:execute)
    switcher.switch_trigger_mode(:left, 'scanner_records', true)
  end
end