require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'
require 'config/test_config.rb'

include RR

describe "PostgreSQLReplication", :shared => true do
  before(:each) do
  end

  it "create_replication_trigger should also work if language plpgsql does not yet exist" do
    session = nil
    begin
      session = Session.new
      session.left.begin_db_transaction
      unless session.left.select_all("select lanname from pg_language where lanname = 'plpgsql'").empty?
        session.left.execute "DROP LANGUAGE plpgsql"
      end
      params = {
        :trigger_name => 'rr_trigger_test',
        :table => 'trigger_test',
        :keys => ['first_id', 'second_id'],
        :log_table => 'rr_pending_changes',
        :key_sep => '|',
        :exclude_rr_activity => false,
      }
      session.left.create_replication_trigger params
      session.left.insert_record 'trigger_test', {
        'first_id' => 1,
        'second_id' => 2,
        'name' => 'bla'
      }

      row = session.left.select_one("select * from rr_pending_changes")
      row.delete 'id'
      row.delete 'change_time'
      row.should == {
        'change_table' => 'trigger_test',
        'change_key' => 'first_id|1|second_id|2',
        'change_new_key' => nil,
        'change_type' => 'I'
      }
      
    ensure
      session.left.rollback_db_transaction if session
    end
  end
end