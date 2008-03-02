require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxySession do
  before(:each) do
    Initializer.configuration = proxied_config
    @session = ProxySession.new Initializer.configuration.left
  end

  it "initialize should connect to the database" do
    @session.connection.active?.should == true
  end
  
  it "destroy should disconnect from the database" do
    @session.destroy

    @session.connection.active?.should == false
  end
  
  it "primary_key_names should return the primary keys of the given table" do
    @session.primary_key_names('scanner_records').should == ['id']
  end
  
  it "cursors should return the current cursor hash or an empty hash if nil" do
    @session.cursors.should == {}
    @session.cursors[:dummy_cursor] = :dummy_cursor
    @session.cursors.should == {:dummy_cursor => :dummy_cursor}    
  end
  
  it "save_cursor should register the provided cursor" do
    @session.save_cursor :dummy_cursor
    
    @session.cursors[:dummy_cursor].should == :dummy_cursor
  end
  
  it "destroy should destroy and unregister any stored cursors" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)
    
    @session.save_cursor cursor
    @session.destroy
    
    @session.cursors.should == {}
  end

  it "destroy_cursor should destroy and unregister the provided cursor" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)
    
    @session.save_cursor cursor
    @session.destroy_cursor cursor
    
    @session.cursors.should == {}
  end
  
  it "create_cursor should create and register the cursor and initiate row fetching" do
    cursor = @session.create_cursor(
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
    @session.column_names('scanner_records').should == ['id', 'name']
  end
  
  it "primary_key_names should return the names of the primary keys of the specified table" do
    @session.primary_key_names('scanner_records').should == ['id']
  end
  
  it "select_one should call select_one of the proxied database connection" do
    @session.connection.should_receive(:select_one).with('dummy_query', 'dummy_name').and_return('dummy_result')
    
    @session.select_one('dummy_query','dummy_name').should == 'dummy_result'
  end
end