require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TypeCastingCursor do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should cache the provided cursor and the retrieved Column objects" do
    cursor = TypeCastingCursor.new Session.new.left, 'extender_type_check', :dummy_org_cursor
    cursor.columns['id'].name.should == 'id'
    cursor.columns['decimal_test'].name.should == 'decimal_test'
    cursor.org_cursor.should == :dummy_org_cursor
  end
  
  it "next_row should delegate next? and clear to the original cursor" do
    session = Session.new
    cursor = session.left.select_cursor(
      :query => "select id from extender_type_check where id = 1",
      :table => "extender_type_check"
    )
    cursor.next?.should be_true
    row = cursor.next_row
    cursor.next?.should be_false
    cursor.clear
  end
  
  it "next_row should cast rows - including uncommon data types - correctly" do
    session = Session.new
    row = session.left.select_record(
      :query => "select id, decimal_test, timestamp, binary_test from extender_type_check where id = 1",
      :table => "extender_type_check"
    )

    # verify that the row fields have been converted to the correct types
    row['id'].should be_an_instance_of(Fixnum)
    row['timestamp'].should be_an_instance_of(Time)
    row['decimal_test'].should be_an_instance_of(BigDecimal)
    row['binary_test'].should be_an_instance_of(String)
    
    # verify that the row values were converted correctly
    row.should == {
      'id' => 1, 
      'decimal_test' => BigDecimal.new("1.234"),
      'timestamp' => Time.local(2007,"nov",10,20,15,1),
      'binary_test' => Marshal.dump(['bla',:dummy,1,2,3])
    }
  end
end
