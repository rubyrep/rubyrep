require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

# All ReplicationExtenders need to pass this spec
describe "ReplicationExtender", :shared => true do
  before(:each) do
  end

  it "create_replication_trigger created triggers should log data changes" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id', 'second_id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
        :exclude_rr_activity => false,
      }
      session.left.create_replication_trigger params

      change_start = Time.now

      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }
      session.left.execute "update trigger_test set second_id = 9 where first_id = 1 and second_id = 2"
      session.left.delete_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 9,
      }

      rows = session.left.connection.select_all("select * from rr_pending_changes order by id")

      # Verify that the timestamps are created correctly
      rows.each do |row|
        Time.parse(row['change_time']).to_i >= change_start.to_i
        Time.parse(row['change_time']).to_i <= Time.now.to_i
      end

      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|2', 'change_new_key' => nil, 'change_type' => 'I'},
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|2', 'change_new_key' => 'first_id|1|second_id|9', 'change_type' => 'U'},
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|9', 'change_new_key' => nil, 'change_type' => 'D'},
      ]
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_pending_changes' if session
      session.left.rollback_db_transaction if session
    end
  end

  it "created triggers should not log rubyrep initiated changes if :exclude_rubyrep_activity is true" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id', 'second_id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
        :exclude_rr_activity => true,
        :activity_table => "rr_running_flags",
      }
      session.left.create_replication_trigger params

      session.left.insert_record 'rr_running_flags', {
        'active' => 1
      }
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }
      session.left.connection.execute('delete from rr_running_flags')
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 3,
        'name' => 'bla'
      }

      rows = session.left.connection.select_all("select * from rr_pending_changes order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
          'change_table' => 'trigger_test',
          'change_key' => 'first_id|1|second_id|3',
          'change_new_key' => nil,
          'change_type' => 'I'
        }]
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_pending_changes' if session
      session.left.rollback_db_transaction if session
    end
  end

  it "created triggers should work with tables having non-combined primary keys" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_extender_no_record',
        :table => 'extender_no_record',
        :keys => ['id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
      }
      session.left.create_replication_trigger params
      session.left.insert_record 'extender_no_record', {
        'id' => 9,
        'name' => 'bla'
      }
      rows = session.left.connection.select_all("select * from rr_pending_changes order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
          'change_table' => 'extender_no_record',
          'change_key' => 'id|9',
          'change_new_key' => nil,
          'change_type' => 'I'
        }]
    ensure
      if session
        session.left.execute 'delete from extender_no_record'
        session.left.execute 'delete from rr_pending_changes'
        session.left.drop_replication_trigger('rr_extender_no_record', 'extender_no_record')
        session.left.rollback_db_transaction
      end
    end
  end

  it "created triggers should work with primary keys holding multi-byte text values" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_scanner_text_key',
        :table => 'scanner_text_key',
        :keys => ['text_id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
      }
      session.left.create_replication_trigger params
      session.left.insert_record 'scanner_text_key', {
        'text_id' => 'よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.',
        'name' => 'bla'
      }
      rows = session.left.connection.select_all("select * from rr_pending_changes order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
          'change_table' => 'scanner_text_key',
          'change_key' => 'text_id|よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.',
          'change_new_key' => nil,
          'change_type' => 'I'
        }]
      found_key = rows[0]['change_key'].sub(/^text_id\|/, '')
      session.left.
        select_one("select * from scanner_text_key where text_id = '#{found_key}'").
        should_not be_nil
    ensure
      if session
        session.left.execute "delete from scanner_text_key where text_id = 'よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.'"
        session.left.execute 'delete from rr_pending_changes'
        session.left.drop_replication_trigger('rr_scanner_text_key', 'scanner_text_key')
        session.left.rollback_db_transaction
      end
    end
  end

  it "replication_trigger_exists? and drop_replication_trigger should work correctly" do
    session = nil
    begin
      session = Session.new
      if session.left.replication_trigger_exists?('rr_trigger_test', 'trigger_test')
        session.left.drop_replication_trigger('rr_trigger_test', 'trigger_test')
      end
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
      }
      session.left.create_replication_trigger params

      session.left.replication_trigger_exists?('rr_trigger_test', 'trigger_test').
        should be_true
      session.left.drop_replication_trigger('rr_trigger_test', 'trigger_test')
      session.left.replication_trigger_exists?('rr_trigger_test', 'trigger_test').
        should be_false
    ensure
      session.left.rollback_db_transaction if session
    end
  end

  it "sequence_values should return an empty hash if table has no sequences" do
    session = Session.new
    session.left.sequence_values('rr', 'scanner_text_key').
      should == {}
  end

  it "sequence_values should return the correct sequence settings" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      session.left.execute 'delete from sequence_test'
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 5, 2,
        left_sequence_values, right_sequence_values, 5
      sequence_value = session.left.sequence_values('rr', 'sequence_test').values[0]
      sequence_value[:increment].should == 5
      (sequence_value[:value] % 5).should == 2
    ensure
      session.left.clear_sequence_setup 'rr', 'sequence_test' if session
      session.left.execute "delete from sequence_test" if session
      session.left.rollback_db_transaction if session
    end
  end

  it "update_sequences should ensure that a table's auto generated ID values have the correct increment and offset" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction

      # Note:
      # Calling ensure_sequence_setup twice with different values to ensure that
      # it is actually does something.

      session.left.execute 'delete from sequence_test'
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 1, 0,
        left_sequence_values, right_sequence_values, 5
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 1

      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 5, 2,
        left_sequence_values, right_sequence_values, 5
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 5
      (id1 % 5).should == 2
    ensure
      session.left.clear_sequence_setup 'rr', 'sequence_test' if session
      session.left.execute "delete from sequence_test" if session
      session.left.rollback_db_transaction if session
    end
  end

  it "update_sequences shoud set the sequence up correctly if the table is not empty" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      session.left.execute 'delete from sequence_test'
      session.left.insert_record 'sequence_test', { 'name' => 'whatever' }
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 2, 0,
        left_sequence_values, right_sequence_values, 5
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 2
    ensure
      session.left.clear_sequence_setup 'rr', 'sequence_test' if session
      session.left.execute "delete from sequence_test" if session
      session.left.rollback_db_transaction if session
    end
  end

  it "update_sequences shoud set the sequence up correctly if the other table is already set up correctly" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      session.right.begin_db_transaction
      session.left.execute 'delete from sequence_test'
      session.left.insert_record 'sequence_test', { 'name' => 'whatever' }
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'

      # setup right table sequence
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.right.update_sequences \
        'rr', 'sequence_test', 2, 1,
        left_sequence_values, right_sequence_values, 5

      # now setup left table sequence and verify result
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 2, 0,
        left_sequence_values, right_sequence_values, 5
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 2
    ensure
      session.left.clear_sequence_setup 'rr', 'sequence_test' if session
      session.right.clear_sequence_setup 'rr', 'sequence_test' if session
      session.left.execute "delete from sequence_test" if session
      session.left.rollback_db_transaction if session
      session.right.rollback_db_transaction if session
    end
  end

  it "clear_sequence_setup should remove custom sequence settings" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      left_sequence_values = session.left.sequence_values 'rr', 'sequence_test'
      right_sequence_values = session.right.sequence_values 'rr', 'sequence_test'
      session.left.update_sequences \
        'rr', 'sequence_test', 2, 0,
        left_sequence_values, right_sequence_values, 5
      session.left.clear_sequence_setup 'rr', 'sequence_test'
      id1, id2 = get_example_sequence_values(session)
      (id2 - id1).should == 1
    ensure
      session.left.clear_sequence_setup 'rr', 'sequence_test' if session
      session.left.execute "delete from sequence_test" if session
      session.left.rollback_db_transaction if session
    end
  end

  it "add_big_primary_key should add a 8 byte, auto incrementing primary key" do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      initializer.silence_ddl_notices(:left) do
        session.left.drop_table 'big_key_test' if session.left.tables.include? 'big_key_test'
        session.left.create_table 'big_key_test'.to_sym
        session.left.add_column 'big_key_test', :name, :string
        session.left.remove_column 'big_key_test', 'id'
        session.left.add_big_primary_key 'big_key_test', 'id'
      end
      # should auto generate the primary key if not manually specified
      session.left.insert_record 'big_key_test', {'name' => 'bla'}
      session.left.select_one("select id from big_key_test where name = 'bla'")['id'].
        to_i.should > 0

      # should allow 8 byte values
      session.left.insert_record 'big_key_test', {'id' => 1e18.to_i, 'name' => 'blub'}
      session.left.select_one("select id from big_key_test where name = 'blub'")['id'].
        to_i.should == 1e18.to_i
    end
  end
end