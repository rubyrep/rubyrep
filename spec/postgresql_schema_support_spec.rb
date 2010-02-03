require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

require File.dirname(__FILE__) + "/../config/test_config.rb"

describe "PostgreSQL schema support" do
  before(:each) do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    config.left[:schema_search_path] = 'rr'
    config.right[:schema_search_path] = 'rr'
    Initializer.configuration = config
  end

  after(:each) do
  end

  if Initializer.configuration.left[:adapter] == 'postgresql'
    it "tables should show the tables from the schema and no others" do
      session = Session.new
      session.left.tables.include?('rr_simple').should be_true
      session.left.tables.include?('scanner_records').should be_false
    end

    it "tables should not show the tables from other schemas" do
      session = Session.new standard_config
      session.left.tables.include?('scanner_records').should be_true
      session.left.tables.include?('rr_simple').should be_false
    end

    it "primary_key_names should work" do
      session = Session.new
      session.left.primary_key_names('rr_simple').should == ['id']
    end

    it "primary_key_names should pick the table in the target schema" do
      session = Session.new
      session.left.primary_key_names('rr_duplicate').should == ['id']
    end

    it "column_names should work" do
      session = Session.new
      session.left.column_names('rr_simple').should == ['id', 'name']
    end

    it "column_names should pick the table in the target schema" do
      session = Session.new
      session.left.column_names('rr_duplicate').should == ['id', 'name']
    end

    it "referenced_tables should work" do
      session = Session.new
      session.left.referenced_tables(['rr_referencing']).should == {
        'rr_referencing' => ['rr_referenced']
      }
    end

    it "table_select_query should work" do
      session = Session.new
      session.left.table_select_query('rr_simple').
        should == 'select "id", "name" from "rr_simple" order by "id"'
    end

    it "TypeCastingCursor should work" do
      session = Session.new
      org_cursor = session.left.select_cursor(
        :query => "select id, name from rr_simple where id = 1",
        :type_cast => false
      )
      cursor = TypeCastingCursor.new session.left, 'rr_simple', org_cursor

      row = cursor.next_row

      row.should == {
        'id' => 1,
        'name' => 'bla'
      }
    end

    it "sequence_values should pick the table in the target schema" do
      session = Session.new
      session.left.sequence_values('rx', 'rr_duplicate').keys.should == ["rr_duplicate_id_seq"]
    end

    it "clear_sequence_setup should pick the table in the target schema" do
      session = nil
      begin
        session = Session.new
        initializer = ReplicationInitializer.new(session)
        session.left.begin_db_transaction
        session.right.begin_db_transaction
        table_pair = {:left => 'rr_duplicate', :right => 'rr_duplicate'}
        initializer.ensure_sequence_setup table_pair, 5, 2, 1
        id1, id2 = get_example_sequence_values(session, 'rr_duplicate')
        (id2 - id1).should == 5
        (id1 % 5).should == 2

        initializer.clear_sequence_setup :left, 'rr_duplicate'
        id1, id2 = get_example_sequence_values(session, 'rr_duplicate')
        (id2 - id1).should == 1
      ensure
        [:left, :right].each do |database|
          initializer.clear_sequence_setup database, 'rr_duplicate' rescue nil if session
          session.send(database).execute "delete from rr_duplicate" rescue nil if session
          session.send(database).rollback_db_transaction rescue nil if session
        end
      end

    end

    it "sequence setup should work" do
      session = nil
      begin
        session = Session.new
        initializer = ReplicationInitializer.new(session)
        session.left.begin_db_transaction
        session.right.begin_db_transaction
        table_pair = {:left => 'rr_sequence_test', :right => 'rr_sequence_test'}
        initializer.ensure_sequence_setup table_pair, 5, 2, 1
        id1, id2 = get_example_sequence_values(session, 'rr_sequence_test')
        (id2 - id1).should == 5
        (id1 % 5).should == 2
      ensure
        [:left, :right].each do |database|
          initializer.clear_sequence_setup database, 'rr_sequence_test' rescue nil if session
          session.send(database).execute "delete from rr_sequence_test" rescue nil if session
          session.send(database).rollback_db_transaction rescue nil if session
        end
      end
    end

    it "clear_sequence_setup should work" do
      session = nil
      begin
        session = Session.new
        initializer = ReplicationInitializer.new(session)
        session.left.begin_db_transaction
        session.right.begin_db_transaction
        table_pair = {:left => 'rr_sequence_test', :right => 'rr_sequence_test'}
        initializer.ensure_sequence_setup table_pair, 5, 2, 2
        initializer.clear_sequence_setup :left, 'rr_sequence_test'
        id1, id2 = get_example_sequence_values(session, 'rr_sequence_test')
        (id2 - id1).should == 1
      ensure
        [:left, :right].each do |database|
          initializer.clear_sequence_setup database, 'rr_sequence_test' if session
          session.send(database).execute "delete from rr_sequence_test" if session
          session.send(database).rollback_db_transaction if session
        end
      end
    end

    it "initializer should create tables in target schema" do
      session = nil
      begin
        config = deep_copy(Initializer.configuration)
        config.options[:rep_prefix] = 'ry'
        session = Session.new config
        session.left.begin_db_transaction

        initializer = ReplicationInitializer.new(session)
        initializer.create_change_log(:left)

        # no exception ==> means table was created in target schema
        session.left.select_one("select id from rr.ry_pending_changes")
      ensure
        session.left.rollback_db_transaction if session
      end
    end

    it "create_trigger, trigger_exists? and drop_trigger should work" do
      session = nil
      begin
        session = Session.new
        initializer = ReplicationInitializer.new(session)
        session.left.begin_db_transaction

        initializer.create_trigger :left, 'rr_trigger_test'
        initializer.trigger_exists?(:left, 'rr_trigger_test').
          should be_true

        # Verify that the trigger can find the pending_changes table even if
        # current search_path does not include it.
        session.left.execute "set search_path = 'public'"
        session.left.execute <<-EOF
          insert into rr.rr_trigger_test(first_id, second_id) values(10, 11)
        EOF
        session.left.execute "set search_path = 'rr'"
        session.left.select_one("select change_key from rx_pending_changes")['change_key'].
          should == "first_id|10|second_id|11"

        initializer.drop_trigger(:left, 'rr_trigger_test')
        initializer.trigger_exists?(:left, 'rr_trigger_test').
          should be_false
      ensure
        session.left.rollback_db_transaction if session
      end
    end

    it "should work with complex search paths" do
      config = deep_copy(standard_config)
      config.left[:schema_search_path] = 'public,rr'
      config.right[:schema_search_path] = 'public,rr'
      session = Session.new(config)

      tables = session.left.tables
      tables.include?('rr_simple').should be_true
      tables.include?('scanner_records').should be_true
    end
  end
end
