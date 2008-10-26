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

      session.left.insert_record 'trigger_test', {
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

  it "ensure_sequence_setup should ensure that a table's auto generated ID values have the correct increment and offset" do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      session.left.begin_db_transaction
      session.right.begin_db_transaction

      # Note:
      # Calling ensure_sequence_setup twice with different values to ensure that
      # it is actually does something.

      initializer.ensure_sequence_setup 'sequence_test', 3, 2
      initializer.ensure_sequence_setup 'sequence_test', 5, 2
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 5
      (id1 % 5).should == 2
    ensure
      [:left, :right].each do |database|
        initializer.clear_sequence_setup database, 'sequence_test' if session
        session.send(database).execute "delete from sequence_test" if session
        session.send(database).rollback_db_transaction if session
      end
    end
  end

  it "clear_sequence_setup should remove custom sequence settings" do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      session.left.begin_db_transaction
      session.right.begin_db_transaction
      initializer.ensure_sequence_setup 'sequence_test', 5, 2
      initializer.clear_sequence_setup :left, 'sequence_test'
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 1
    ensure
      [:left, :right].each do |database|
        initializer.clear_sequence_setup database, 'sequence_test' if session
        session.send(database).execute "delete from sequence_test" if session
        session.send(database).rollback_db_transaction if session
      end
    end
  end

  it "replication_log_exists? should return true if replication log exists" do
    config = deep_copy(standard_config)
    initializer = ReplicationInitializer.new(Session.new(config))
    initializer.replication_log_exists?(:left).should be_true
    config.options[:rep_prefix] = 'r2'
    initializer = ReplicationInitializer.new(Session.new(config))
    initializer.replication_log_exists?(:left).should be_false
  end

  it "create_replication_log / drop_replication_log should create / drop the replication log" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'r2'
    initializer = ReplicationInitializer.new(Session.new(config))
    initializer.drop_replication_log(:left) if initializer.replication_log_exists?(:left)

    $stderr.stub! :write
    initializer.replication_log_exists?(:left).should be_false
    initializer.create_replication_log(:left)
    initializer.replication_log_exists?(:left).should be_true
    initializer.drop_replication_log(:left)
    initializer.replication_log_exists?(:left).should be_false
  end
end
