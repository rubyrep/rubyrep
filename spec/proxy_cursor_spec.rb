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
  
  it "construct_query should handle queries without any conditions" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    ProxyCursor.new(connection, 'scanner_records').construct_query \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records'\
        order by 'id'")
  end
  
  it "construct_query should handle queries with only a from condition" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    ProxyCursor.new(connection, 'scanner_records').construct_query(:from => {'id' => 1}) \
      .should =~ sql_to_regexp("\
         select 'id', 'name' from 'scanner_records' \
         where ('id') >= (1) order by 'id'")
  end
  
  it "construct_query should handle queries with only a to condition" do
    connection = ProxyConnection.new Initializer.configuration.left

    ProxyCursor.new(connection, 'scanner_text_key').construct_query(:to => {'text_id' => 'k1'}) \
      .should =~ sql_to_regexp("\
         select 'text_id', 'name' from 'scanner_text_key' \
         where ('text_id') <= ('k1') order by 'text_id'")
  end
  
  it "construct_query should handle queries with both from and to conditions" do
    connection = ProxyConnection.new Initializer.configuration.left

    ProxyCursor.new(connection, 'scanner_records').construct_query(:from => {'id' => 0}, :to => {'id' => 1}) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') <= (1) order by 'id'")
  end
  
  it "construct_query should handle queries for specific rows" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    ProxyCursor.new(connection, 'scanner_records').construct_query(
      :row_keys => [{'id' => 0}, {'id' => 1}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') in ((0), (1)) order by 'id'")
  end
  
  it "construct_query should handle queries for specific rows with the row array actually being empty" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    ProxyCursor.new(connection, 'scanner_records').construct_query(:row_keys => []) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where false order by 'id'")
  end
  
  it "construct_query should handle queries for specific rows in combination with other conditions" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    ProxyCursor.new(connection, 'scanner_records').construct_query(
      :from => {'id' => 0},
      :row_keys => [{'id' => 1}, {'id' => 2}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') in ((1), (2)) order by 'id'")
  end
  
  it "construct_query should handle tables with combined primary keys" do
    connection = ProxyConnection.new Initializer.configuration.left

    ProxyCursor.new(connection, 'extender_combined_key').construct_query(
      :from => {'first_id' => 0, 'second_id' => 1}, 
      :to => {'first_id' => 2, 'second_id' => 3}) \
      .should =~ sql_to_regexp("\
        select 'first_id', 'second_id' from 'extender_combined_key' \
        where ('first_id', 'second_id') >= (0, 1) \
        and ('first_id', 'second_id') <= (2, 3) \
        order by 'first_id', 'second_id'")
  end
  
  it "construct_query should quote column values" do
    connection = ProxyConnection.new Initializer.configuration.left
    
    cursor = ProxyCursor.new(connection, 'scanner_text_key')
    cursor.construct_query(:from => {'text_id' => 'a'}, :to => {'text_id' => 'b'}) \
      .should match(/'a'.*'b'/)
    # additional check that the quoted query actually works
    results = cursor.prepare_fetch(:from => {'text_id' => 'a'}, :to => {'text_id' => 'b'})
    results.next_row.should == {'text_id' => 'a', 'name' => 'Alice'}
    results.next_row.should == {'text_id' => 'b', 'name' => 'Bob'}
    results.next?.should be_false
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
    cursor.cursor.next_row.should == {'first_id' => 1, 'second_id' => 1}
    cursor.cursor.next_row.should == {'first_id' => 1, 'second_id' => 2}
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
