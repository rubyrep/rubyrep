require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyConnection do
  before(:each) do
    Initializer.configuration = proxied_config
    @connection = ProxyConnection.new Initializer.configuration.left
  end

  it "initialize should connect to the database" do
    @connection.connection.active?.should == true
  end
  
  it "destroy should disconnect from the database" do
    @connection.destroy

    @connection.connection.active?.should == false
  end
  
  it "cursors should return the current cursor hash or an empty hash if nil" do
    @connection.cursors.should == {}
    @connection.cursors[:dummy_cursor] = :dummy_cursor
    @connection.cursors.should == {:dummy_cursor => :dummy_cursor}    
  end
  
  it "save_cursor should register the provided cursor" do
    @connection.save_cursor :dummy_cursor
    
    @connection.cursors[:dummy_cursor].should == :dummy_cursor
  end
  
  it "destroy should destroy and unregister any stored cursors" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)
    
    @connection.save_cursor cursor
    @connection.destroy
    
    @connection.cursors.should == {}
  end

  it "destroy_cursor should destroy and unregister the provided cursor" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)
    
    @connection.save_cursor cursor
    @connection.destroy_cursor cursor
    
    @connection.cursors.should == {}
  end
  
  it "create_cursor should create and register the cursor and initiate row fetching" do
    cursor = @connection.create_cursor(
      ProxyRowCursor, 
      'scanner_records',
      :from => {'id' => 2},
      :to => {'id' => 2}
    )

    cursor.should be_an_instance_of(ProxyRowCursor)
    cursor.next_row_keys_and_checksum[0].should == {'id' => 2} # verify that 'from' range was used
    cursor.next?.should be_false # verify that 'to' range was used
  end
  
  it "column_names should return the column names of the specified table" do
    @connection.column_names('scanner_records').should == ['id', 'name']
  end
  
  it "column_names should cache the column names" do
    @connection.column_names('scanner_records')
    @connection.column_names('scanner_text_key')
    @connection.connection.should_not_receive(:columns)
    @connection.column_names('scanner_records').should == ['id', 'name']
  end
  
  it "primary_key_names should return the correct primary keys" do
    @connection.primary_key_names('scanner_records').should == ['id']
  end

  it "primary_key_names should cache the primary primary keys" do
    @connection.connection.should_receive(:primary_key_names) \
      .with('dummy_table').once.and_return(['dummy_key'])
    @connection.connection.should_receive(:primary_key_names) \
      .with('dummy_table2').once.and_return(['dummy_key2'])
    
    @connection.primary_key_names('dummy_table').should == ['dummy_key']
    @connection.primary_key_names('dummy_table2').should == ['dummy_key2']
    @connection.primary_key_names('dummy_table').should == ['dummy_key']
  end

  it "table_select_query should handle queries without any conditions" do
    @connection.table_select_query('scanner_records') \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records'\
        order by 'id'")
  end
  
  it "table_select_query should handle queries with only a from condition" do
    @connection.table_select_query('scanner_records', :from => {'id' => 1}) \
      .should =~ sql_to_regexp("\
         select 'id', 'name' from 'scanner_records' \
         where ('id') >= (1) order by 'id'")
  end
  
  it "table_select_query should handle queries with only a to condition" do
    @connection.table_select_query('scanner_text_key', :to => {'text_id' => 'k1'}) \
      .should =~ sql_to_regexp("\
         select 'text_id', 'name' from 'scanner_text_key' \
         where ('text_id') <= ('k1') order by 'text_id'")
  end
  
  it "table_select_query should handle queries with both from and to conditions" do
    @connection.table_select_query('scanner_records', 
      :from => {'id' => 0}, :to => {'id' => 1}) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') <= (1) order by 'id'")
  end
  
  it "table_select_query should handle queries for specific rows" do
    @connection.table_select_query('scanner_records',
      :row_keys => [{'id' => 0}, {'id' => 1}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') in ((0), (1)) order by 'id'")
  end
  
  it "table_select_query should handle queries for specific rows with the row array actually being empty" do
    @connection.table_select_query('scanner_records', :row_keys => []) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where false order by 'id'")
  end
  
  it "table_select_query should handle queries for specific rows in combination with other conditions" do
    @connection.table_select_query('scanner_records',
      :from => {'id' => 0},
      :row_keys => [{'id' => 1}, {'id' => 2}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') in ((1), (2)) order by 'id'")
  end
  
  it "table_select_query should handle tables with combined primary keys" do
    @connection.table_select_query('extender_combined_key',
      :from => {'first_id' => 0, 'second_id' => 1}, 
      :to => {'first_id' => 2, 'second_id' => 3}) \
      .should =~ sql_to_regexp("\
        select 'first_id', 'second_id' from 'extender_combined_key' \
        where ('first_id', 'second_id') >= (0, 1) \
        and ('first_id', 'second_id') <= (2, 3) \
        order by 'first_id', 'second_id'")
  end
  
  it "table_select_query should quote column values" do
    select_options = {:from => {'text_id' => 'a'}, :to => {'text_id' => 'b'}}
    
    @connection.table_select_query('scanner_text_key', select_options) \
      .should match(/'a'.*'b'/)
    
    # additional check that the quoted query actually works
    cursor = ProxyCursor.new(@connection, 'scanner_text_key')
    results = cursor.prepare_fetch(select_options)
    results.next_row.should == {'text_id' => 'a', 'name' => 'Alice'}
    results.next_row.should == {'text_id' => 'b', 'name' => 'Bob'}
    results.next?.should be_false
  end
  
  unless RUBY_PLATFORM =~ /java/
    # This test unfortunately does not run correctly under java as the columns 
    # are written in the query in a different sequence (hash data sorting problem).
    # However the insert queries are also under jruby verified by actually
    # running them (refer to the next spec)
    it "table_insert_query should return the correct SQL query" do
      @connection.table_insert_query('scanner_records', 'id' => 9, 'name' => 'bla') \
        .should =~ sql_to_regexp(%q!insert into "scanner_records"("name", "id") values('bla', 9)!)
    end
  end
  
  it "queries returned by table_insert_query should execute successfully" do
    @connection.begin_db_transaction
    begin
      query = @connection.table_insert_query('scanner_records', 'id' => 9, 'name' => 'bla')
      @connection.execute query
      @connection.select_one("select * from scanner_records where id = 9") \
        .should == {'id' => '9', 'name' => 'bla'}
    ensure
      @connection.rollback_db_transaction
    end
  end

  it "queries returned by table_insert_query should write uncommon data types correctly" do
    @connection.begin_db_transaction
    begin
      test_data = {
        'id' => 2, 
        'decimal_test' => 1.234,
        'timestamp' => Time.local(2008,"jun",9,20,15,1),
        'multi_byte' => "よろしくお願(ねが)いします yoroshiku onegai shimasu: I humbly ask for your favor.",
        'binary_test' => Marshal.dump(['bla',:dummy,1,2,3]),
        'text_test' => 'dummy text'
      }
      query = @connection.table_insert_query('extender_type_check', test_data)
      @connection.execute query

      org_cursor = @connection.select_cursor("select * from extender_type_check where id = 2")
      cursor = TypeCastingCursor.new @connection, 'extender_type_check', org_cursor
      result_data = cursor.next_row
      result_data.should == test_data
    ensure
      @connection.rollback_db_transaction
    end
  end
end