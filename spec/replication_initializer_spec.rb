require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationInitializer do
  before(:each) do
    Initializer.configuration = standard_config
  end

  after(:each) do
  end

  it "initializer should store the session" do
    session = Session.new
    initializer = ReplicationInitializer.new session
    initializer.session.should == session
  end

  it "options should return the table specific options if table is given" do
    session = Session.new deep_copy(Initializer.configuration)
    initializer = ReplicationInitializer.new session
    session.configuration.should_receive(:options_for_table).
      with('my_table').
      and_return(:dummy_options)
    initializer.options('my_table').should == :dummy_options
  end

  it "options should return the general options if no table is given" do
    session = Session.new deep_copy(Initializer.configuration)
    initializer = ReplicationInitializer.new session
    session.configuration.should_receive(:options).
      and_return(:dummy_options)
    initializer.options.should == :dummy_options
  end

  it "create_trigger should create a working trigger" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      initializer = ReplicationInitializer.new(session)
      initializer.create_trigger(:left, 'trigger_test')

      create_row session.left.connection, 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }

      row = session.left.select_one("select * from rr_change_log")
      row.delete 'id'
      row.delete 'change_time'
      row.should == {
        'change_table' => 'trigger_test',
        'change_key' => 'first_id|1|second_id|2',
        'change_org_key' => nil,
        'change_type' => 'I'
      }
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_change_log' if session
      session.left.rollback_db_transaction if session
    end
  end

  it "trigger_exists? and drop_trigger should work correctly" do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      if initializer.trigger_exists?(:left, 'trigger_test')
        initializer.drop_trigger(:left, 'trigger_test')
      end
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id'],
        :log_table => 'rr_change_log',
        :key_sep => '|',
      }
      initializer.create_trigger :left, 'trigger_test'

      initializer.trigger_exists?(:left, 'trigger_test').
        should be_true
      initializer.drop_trigger(:left, 'trigger_test')
      initializer.trigger_exists?(:left, 'trigger_test').
        should be_false
    ensure
      session.left.rollback_db_transaction if session
    end
  end
end
