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
  end
end
