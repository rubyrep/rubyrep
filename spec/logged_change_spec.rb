require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe LoggedChange do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should store session and database" do
    session = Session.new
    loader = LoggedChangeLoader.new session, :left
    change = LoggedChange.new loader
    change.session.should == session
    change.database.should == :left
  end

  it "load_specified should load the specified change" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'right_table',
        'change_key' => 'id|2',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '2'}

      change.table.should == 'left_table'
      change.type.should == :insert
      change.key.should == {'id' => '2'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should accept a column_name => value hash as key" do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables "scanner_records", :key => ['id1', 'id2']

    session = Session.new config
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id1|1|id2|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'scanner_records', {'id1' => 1, 'id2' => 2}

      change.table.should == 'scanner_records'
      change.type.should == :insert
      change.key.should == {'id1' => '1', 'id2' => '2'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should delete loaded changes from the database" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => 1}

      session.left.
        select_one("select * from rr_pending_changes where change_key = 'id|1'").
        should be_nil
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should set first_change_at and last_changed_at correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      t1 = 5.seconds.ago
      t2 = 5.seconds.from_now
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => t1
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|1',
        'change_type' => 'U',
        'change_time' => t2
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => 1}

      (change.first_changed_at - t1).abs.should < 1
      (change.last_changed_at - t2).abs.should < 1
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should follow primary key updates correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_new_key' => 'id|3',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => 1}

      change.type.should == :update
      change.key.should == {'id' => 1}
      change.new_key.should == {'id' => '3'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should recognize if changes cancel each other out" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '1'}

      change.type.should == :no_change
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_specified should transist states correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin

      # first test case
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '1'}
      change.type.should == :insert
      change.key.should == {'id' => '2'}

      # second test case
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|5',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|5',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader.update :forced => true
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '5'}
      change.type.should == :update
      change.key.should == {'id' => '5'}
      change.new_key.should == {'id' => '5'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "amend should work if there were no changes" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'scanner_records', {'id' => '1'}

      change.table.should == 'scanner_records'
      change.type.should == :insert
      change.key.should == {'id' => '1'}

      change.load

      change.table.should == 'scanner_records'
      change.type.should == :insert
      change.key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "amend should work if the current type is :no_change" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'scanner_records', {'id' => '1'}

      change.table.should == 'scanner_records'
      change.type.should == :no_change
      change.key.should == {'id' => '1'}

      change.load

      change.table.should == 'scanner_records'
      change.type.should == :no_change
      change.key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "amend should amend the change correctly" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'left_table', {
        :id => '1',
        :name => 'bla'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|1',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '1'}
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      loader.update :forced => true
      change.load

      change.table.should == 'left_table'
      change.type.should == :delete
      change.key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "amend should support primary key updates" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'left_table', {
        :id => '1',
        :name => 'bla'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_new_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_specified 'left_table', {'id' => '1'}
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_new_key' => 'id|3',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      loader.update :forced => true
      change.load

      change.table.should == 'left_table'
      change.type.should == :update
      change.key.should == {'id' => '1'}
      change.new_key.should == {'id' => '3'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "key_from_raw_key should return the correct column_name => value hash for the given key" do
    loader = LoggedChangeLoader.new Session.new, :left
    change = LoggedChange.new loader
    change.key_to_hash("a|1|b|2").should == {
      'a' => '1',
      'b' => '2'
    }
  end

  it "key_from_raw_key should work with multi character key_sep strings" do
    loader = LoggedChangeLoader.new Session.new, :left
    change = LoggedChange.new loader
    change.stub!(:key_sep).and_return('BLA')
    change.key_to_hash("aBLA1BLAbBLA2").should == {
      'a' => '1',
      'b' => '2'
    }
  end

  it "load_oldest should not load a change if none available" do
    loader = LoggedChangeLoader.new Session.new, :left
    change = LoggedChange.new loader
    change.should_not_receive :load_specified
    change.load_oldest
  end

  it "load_oldest should load the oldest available change" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_oldest

      change.key.should == {'id' => '1'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "load_oldest should skip irrelevant changes" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      loader = LoggedChangeLoader.new session, :left
      change = LoggedChange.new loader
      change.load_oldest

      change.type.should == :insert
      change.key.should == {'id' => '2'}
    ensure
      session.left.rollback_db_transaction
    end
  end

  it "to_yaml should blank out session and loader" do
    session = Session.new
    loader = LoggedChangeLoader.new session, :left
    change = LoggedChange.new loader
    yaml = change.to_yaml
    yaml.should_not =~ /session/
    yaml.should_not =~ /loader/
  end
end
