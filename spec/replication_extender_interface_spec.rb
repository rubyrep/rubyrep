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
        :log_table => 'rr_change_log',
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

      rows = session.left.connection.select_all("select * from rr_change_log order by id")

      # Verify that the timestamps are created correctly
      rows.each do |row|
        Time.parse(row['change_time']).to_i >= change_start.to_i
        Time.parse(row['change_time']).to_i <= Time.now.to_i
      end

      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|2', 'change_org_key' => nil, 'change_type' => 'I'},
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|9', 'change_org_key' => 'first_id|1|second_id|2', 'change_type' => 'U'},
        {'change_table' => 'trigger_test', 'change_key' => 'first_id|1|second_id|9', 'change_org_key' => nil, 'change_type' => 'D'},
      ]
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_change_log' if session
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
        :log_table => 'rr_change_log',
        :key_sep => '|',
        :exclude_rr_activity => true,
        :activity_table => "rr_active",
      }
      session.left.create_replication_trigger params

      session.left.insert_record 'rr_active', {
        'active' => 1
      }
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }
      session.left.connection.execute('delete from rr_active')
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 3,
        'name' => 'bla'
      }

      rows = session.left.connection.select_all("select * from rr_change_log order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
          'change_table' => 'trigger_test',
          'change_key' => 'first_id|1|second_id|3',
          'change_org_key' => nil,
          'change_type' => 'I'
        }]
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_change_log' if session
      session.left.rollback_db_transaction if session
    end
  end

  it "created triggers should work with tables having non-combined primary keys" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id'],
        :log_table => 'rr_change_log',
        :key_sep => '|',
      }
      session.left.create_replication_trigger params
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }
      rows = session.left.connection.select_all("select * from rr_change_log order by id")
      rows.each {|row| row.delete 'id'; row.delete 'change_time'}
      rows.should == [{
          'change_table' => 'trigger_test',
          'change_key' => 'first_id|1',
          'change_org_key' => nil,
          'change_type' => 'I'
        }]
    ensure
      session.left.execute 'delete from trigger_test' if session
      session.left.execute 'delete from rr_change_log' if session
      session.left.rollback_db_transaction if session
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
        :log_table => 'rr_change_log',
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
end