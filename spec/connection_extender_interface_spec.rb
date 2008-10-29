require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

# All ConnectionExtenders need to pass this spec
describe "ConnectionExtender", :shared => true do
  before(:each) do
  end

  it "primary_key_names should return primary key names ordered as per primary key index" do
    session = Session.new
    session.left.primary_key_names('extender_combined_key').should == ['first_id', 'second_id']
    
    session.left.primary_key_names('extender_inverted_combined_key') \
      .should == ['second_id', 'first_id']
  end
  
  it "primary_key_names should return an empty array for tables without any primary key" do
    session = Session.new
    session.left.primary_key_names('extender_without_key') \
      .should == []
  end
  
  it "primary_key_names called for a non-existing table should throw an exception" do
    session = Session.new
    lambda {session.left.primary_key_names('non_existing_table')} \
      .should raise_error(RuntimeError, 'table does not exist')
  end

  it "referenced_tables should identify the correct table dependencies" do
    session = Session.new
    referenced_tables = session.left.referenced_tables(['scanner_records', 'referencing_table'])
    referenced_tables.size.should == 2
    referenced_tables['scanner_records'].should == []
    referenced_tables['referencing_table'].sort.
      should == ["referenced_table", "referenced_table2"]
  end
  
  it "referenced_tables should return those tables without primary key" do
    session = Session.new
    referenced_tables = session.left.referenced_tables(['table_with_manual_key'])
    referenced_tables.should == {'table_with_manual_key' => []}
  end

  it "select_cursor should handle zero result queries" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_no_record"
    result.next?.should be_false
  end
  
  it "select_cursor should allow iterating through records" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_one_record"
    result.next?.should be_true
    result.next_row.should == {'id' => "1", 'name' => 'Alice'}
  end
  
  it "select_cursor next_row should raise if there are no records" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_no_record"
    lambda {result.next_row}.should raise_error(RuntimeError, 'no more rows available')  
  end
  
  it "select_cursor next_row should handle multi byte characters correctly" do
    session = Session.new
    result = session.left.select_cursor "select id, multi_byte from extender_type_check"
    row = result.next_row
    row.should == {
      'id' => "1", 
      'multi_byte' => "よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor."
    }
  end
  
  it "select_cursor should read null values correctly" do
    session = Session.new
    result = session.left.select_cursor(
      "select first_id, second_id, name from extender_combined_key
       where (first_id, second_id) = (3, 1)")
    result.next_row.should == {'first_id' => '3', 'second_id' => '1', 'name' => nil}
  end
  
  it "should read and write binary data correctly" do
    session = Session.new

    org_data = Marshal.dump(['bla',:dummy,1,2,3])
    result_data = nil
    begin
      session.left.begin_db_transaction
      sql = "insert into extender_type_check(id, binary_test) values(2, '#{org_data}')"
      session.left.execute sql

      org_cursor = session.left.select_cursor("select id, binary_test from extender_type_check where id = 2")
      cursor = TypeCastingCursor.new session.left, 'extender_type_check', org_cursor
      result_data = cursor.next_row['binary_test']
    ensure
      session.left.rollback_db_transaction
    end
    result_data.should == org_data
  end
  
  it "should read and write text data correctly" do
    session = Session.new

    org_data = "よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor."
    result_data = nil
    begin
      session.left.begin_db_transaction
      sql = "insert into extender_type_check(id, text_test) values(2, '#{org_data}')"
      session.left.execute sql

      org_cursor = session.left.select_cursor("select id, text_test from extender_type_check where id = 2")
      cursor = TypeCastingCursor.new session.left, 'extender_type_check', org_cursor
      result_data = cursor.next_row['text_test']
    ensure
      session.left.rollback_db_transaction
    end
    result_data.should == org_data
  end
  
  it "cursors returned by select_cursor should support clear" do
    session = Session.new
    result = session.left.select_cursor "select * from extender_one_record"
    result.next?.should be_true
    result.should respond_to(:clear)
    result.clear
  end
end