require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe "PostgreSQL schema support" do
  before(:each) do
    config = deep_copy(standard_config)
    config.left[:schema_search_path] = 'rr'
    Initializer.configuration = config
  end

  after(:each) do
  end

  if ENV['RR_TEST_DB'] != @org_test_db.to_s
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

    it "TypeCasingCursor should work" do
      session = Session.new
      org_cursor = session.left.select_cursor("select id, name from rr_simple where id = 1")
      cursor = TypeCastingCursor.new session.left, 'rr_simple', org_cursor

      row = cursor.next_row

      row.should == {
        'id' => 1,
        'name' => 'bla'
      }
    end

  end
end
