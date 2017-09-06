require 'spec_helper'

include RR

describe "PostgreSQL support" do
  before(:each) do
    Initializer.configuration = standard_config
  end

  after(:each) do
  end

  if standard_config.left[:adapter] == 'postgresql'

    it "should read & write microsecond times" do
      session = nil
      begin
        session = Session.new
        session.left.begin_db_transaction
        session.left.insert_record('extender_type_check',
          {'id' => 2, 'timestamp' => Time.local(2009, "feb", 16, 20, 48, 1, 543)}
        )

        cursor = session.left.select_cursor(
          :query => "select id, timestamp from extender_type_check where id = 2",
          :table => :extender_type_check
        )

        row = cursor.next_row
        row['timestamp'].usec.should == 543
        cursor.clear
      ensure
        session.left.rollback_db_transaction if session
      end
    end

    it "should not round microsecond times to incorrect value" do
      session = nil
      begin
        session = Session.new
        session.left.begin_db_transaction
        session.left.insert_record('extender_type_check',
          {'id' => 2, 'timestamp' => Time.local(2009, "feb", 16, 13, 37, 11, 126291)}
        )

        cursor = session.left.select_cursor(
          :query => "select id, timestamp from extender_type_check where id = 2",
          :table => :extender_type_check
        )

        row = cursor.next_row
        row['timestamp'].usec.should == 126291
        cursor.clear
      ensure
        session.left.rollback_db_transaction if session
      end
    end

    it "should read & write postgres specific types" do
      session = nil
      begin
        session = Session.new
        session.left.begin_db_transaction


        # first: read test (by writing the data via literal constants)
        session.left.insert_record('postgres_types',
          {
            'id' => 1,
            'text_array' => '{a, b}',
            'float_array' => '{1.23, 4.56}',
            'json' => '{"a": 1, "b": 2}',
            'jsonb' => '{"a": 5, "b": 6}'
          }
        )

        cursor = session.left.select_cursor(
          :query => "select id, text_array, float_array, json, jsonb from postgres_types where id = 1",
          :table => :postgres_types
        )

        row = cursor.next_row
        row['text_array'].should == ['a', 'b']
        row['float_array'].should == [1.23, 4.56]
        row['json'].should == {'a' => 1, 'b' => 2}
        row['jsonb'].should == {'a' => 5, 'b' => 6}
        cursor.clear


        # second: write test
        session.left.insert_record('postgres_types',
          {
            'id' => 2,
            'text_array' => ['c', 'd'],
            'float_array' => [0.0000000000000000001, -1000000000000000000.1],
            'json' => {'c' => 10, 'd' => 20},
            'jsonb' => {'c' => 50, 'd' => 60, 'e' => [{'f' => 'ff'}, 5.7]}
          }
        )

        cursor = session.left.select_cursor(
          :query => "select id, text_array, float_array, json, jsonb from postgres_types where id = 2",
          :table => :postgres_types
        )

        row = cursor.next_row
        row['text_array'].should == ['c', 'd']
        row['float_array'].should == [0.0000000000000000001, -1000000000000000000.1]
        row['json'].should == {'c' => 10, 'd' => 20}
        row['jsonb'].should == {'c' => 50, 'd' => 60, 'e' => [{'f' => 'ff'}, 5.7]}
        cursor.clear
      ensure
        session.left.rollback_db_transaction if session
      end
    end
  end
end
