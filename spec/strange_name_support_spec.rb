require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe "Unusual table and column name support" do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should be able to insert, update and delete records" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      select_row = Proc.new {
        session.left.
          select_one(
          "select #{session.left.quote_column_name(STRANGE_COLUMN)}
           from #{session.left.quote_table_name(STRANGE_TABLE)}")
      }

      session.left.insert_record STRANGE_TABLE, 'id' => 1, STRANGE_COLUMN => 'bla'
      select_row.call[STRANGE_COLUMN].should == 'bla'

      session.left.update_record STRANGE_TABLE, 'id' => 1, STRANGE_COLUMN => 'blub'
      select_row.call[STRANGE_COLUMN].should == 'blub'

      session.left.delete_record STRANGE_TABLE, 'id' => 1
      select_row.call.should be_nil
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "should be able to identify primary keys" do
    session = Session.new
    session.left.primary_key_names(STRANGE_TABLE).should == ['id']
  end

  it "should be able to identify referenced tables" do
    session = Session.new
    referenced_tables = session.left.referenced_tables([STRANGE_TABLE])
    referenced_tables.size.should == 1
    referenced_tables[STRANGE_TABLE].sort.
      should == ["referenced_table"]
  end

  it "should support sequence operations" do
    session = Session.new
    begin
      left_sequence_values = session.left.sequence_values('rr', STRANGE_TABLE)
      right_sequence_values = session.right.sequence_values('rr', STRANGE_TABLE)
      session.left.update_sequences(
        'rr', STRANGE_TABLE, 10, 7, left_sequence_values, right_sequence_values, 1)

      sequence_props = session.left.sequence_values('rr', STRANGE_TABLE).values.first
      sequence_props[:increment].should == 10
      (sequence_props[:value] % 10).should == 7

      session.left.clear_sequence_setup 'rr', STRANGE_TABLE

      session.left.sequence_values('rr', STRANGE_TABLE).values.first[:increment].should == 1
    ensure
      session.left.clear_sequence_setup 'rr', STRANGE_TABLE
    end
  end

  it "should support trigger operations for strange tables" do
    trigger_name = 'rr_' + STRANGE_TABLE
    session = Session.new
    begin
      session.left.replication_trigger_exists?(trigger_name, STRANGE_TABLE).should be_false
      session.left.create_replication_trigger({
          :trigger_name => trigger_name,
          :table => STRANGE_TABLE,
          :keys => ['id'],
          :log_table => 'rr_pending_changes',
          :activity_table => 'rr_running_flags',
          :key_sep => '|',
          :exclude_rr_activity => true
        })
      session.left.replication_trigger_exists?(trigger_name, STRANGE_TABLE).should be_true
      session.left.insert_record STRANGE_TABLE, {
        'id' => 11,
        STRANGE_COLUMN => 'blub'
      }
      log_record = session.left.select_one(
        "select * from rr_pending_changes where change_table = '#{STRANGE_TABLE}'")
      log_record['change_key'].should == 'id|11'
      log_record['change_type'].should == 'I'

      session.left.drop_replication_trigger trigger_name, STRANGE_TABLE
      session.left.replication_trigger_exists?(trigger_name, STRANGE_TABLE).should be_false
    ensure
      if session.left.replication_trigger_exists?(trigger_name, STRANGE_TABLE)
        session.left.drop_replication_trigger trigger_name, STRANGE_TABLE
      end
      session.left.execute "delete from #{session.left.quote_table_name(STRANGE_TABLE)}"
      session.left.execute "delete from rr_pending_changes"
    end
  end

  it "should support trigger operations for table with strange primary keys" do
    trigger_name = 'rr_table_with_strange_key'
    session = Session.new
    begin
      session.left.replication_trigger_exists?(trigger_name, :table_with_strange_key).should be_false
      session.left.create_replication_trigger({
          :trigger_name => trigger_name,
          :table => :table_with_strange_key,
          :keys => [STRANGE_COLUMN],
          :log_table => 'rr_pending_changes',
          :activity_table => 'rr_running_flags',
          :key_sep => '|',
          :exclude_rr_activity => true
        })
      session.left.replication_trigger_exists?(trigger_name, :table_with_strange_key).should be_true
      session.left.insert_record 'table_with_strange_key', {
        STRANGE_COLUMN => '11'
      }
      log_record = session.left.select_one(
        "select * from rr_pending_changes where change_table = 'table_with_strange_key'")
      log_record['change_key'].should == "#{STRANGE_COLUMN}|11"
      log_record['change_type'].should == 'I'

      session.left.drop_replication_trigger trigger_name, :table_with_strange_key
      session.left.replication_trigger_exists?(trigger_name, :table_with_strange_key).should be_false
    ensure
      if session.left.replication_trigger_exists?(trigger_name, :table_with_strange_key)
        session.left.drop_replication_trigger trigger_name, :table_with_strange_key
      end
      session.left.execute "delete from #{session.left.quote_table_name(:table_with_strange_key)}"
      session.left.execute "delete from rr_pending_changes"
    end
  end
end