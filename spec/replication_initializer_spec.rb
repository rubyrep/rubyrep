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
        'change_type' => 'I'
      }
    ensure
      session.left.rollback_db_transaction if session
    end
  end
end
