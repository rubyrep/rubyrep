require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyCursor do
  before(:each) do
    Initializer.configuration = proxied_config
  end

  it "initialize should store session and table and cache the primary keys of table" do
    connection = create_mock_proxy_connection 'dummy_table', ['dummy_key']
    
    cursor = ProxyCursor.new connection, 'dummy_table'
    
    cursor.connection.should == connection
    cursor.table.should == 'dummy_table'
    cursor.primary_key_names.should == ['dummy_key']
  end
  
  it "prepare_fetch should initiate the query and wrap it for type casting" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    cursor = ProxyCursor.new(connection, 'scanner_records')
    cursor.prepare_fetch
    cursor.cursor.should be_an_instance_of(TypeCastingCursor)
    cursor.cursor.next_row.should == {'id' => 1, 'name' => 'Alice - exists in both databases'}
  end
  
  it "prepare_fetch called with option :row_keys should initiate the correct query" do
    # Note: I am testing row_keys exclusively to make sure that this type of 
    #       sub query will work correctly on all supported databases
    connection = ProxyConnection.new Initializer.configuration.left
    
    cursor = ProxyCursor.new(connection, 'extender_combined_key')
    cursor.prepare_fetch :row_keys => [
      {'first_id' => 1, 'second_id' => 1}, 
      {'first_id' => 1, 'second_id' => 2}
    ]
    cursor.cursor.next_row.should == {'first_id' => 1, 'second_id' => 1, 'name' => 'aa'}
    cursor.cursor.next_row.should == {'first_id' => 1, 'second_id' => 2, 'name' => 'ab'}
    cursor.cursor.next?.should == false
  end

  
  it "destroy should clear and nil the cursor" do
    connection = create_mock_proxy_connection 'dummy_table', ['dummy_key']
    cursor = ProxyCursor.new connection, 'dummy_table'
    
    table_cursor = mock("DBCursor")
    table_cursor.should_receive(:clear)
    cursor.cursor = table_cursor
    
    cursor.destroy  
    cursor.cursor.should be_nil
  end  
end
