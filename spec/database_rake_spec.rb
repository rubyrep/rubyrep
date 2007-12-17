require File.dirname(__FILE__) + '/spec_helper.rb'
load File.dirname(__FILE__) + '/../tasks/database.rake'

describe "database.rake" do
  before(:each) do
  end

  it "create_database should create a non-existing database" do
    RR::ConnectionExtenders.should_receive(:db_connect).and_raise("something")
    should_receive("`").with("createdb \"dummy\" -E utf8")
    
    create_database :adapter => "postgresql", :database => "dummy"
  end
  
  it "create_database should not try to create existing databases" do
    RR::ConnectionExtenders.should_receive(:db_connect)
    should_receive(:puts).with("database existing_db already exists")
    
    create_database :adapter => 'postgresql', :database => "existing_db"
  end

  it "create_database should complain about unsupported adapters" do
    should_receive(:puts).with("adapter unsupported_adapter not supported")
    
    create_database :adapter => "unsupported_adapter"
  end
  
  it "drop_database should drop a PostgreSQL database" do
    should_receive("`").with("dropdb \"dummy\"")
    
    drop_database :adapter => "postgresql", :database => "dummy"
  end
  
  it "drop_database should complain about unsupported adapters" do
    should_receive(:puts).with("adapter unsupported_adapter not supported")

    drop_database :adapter => "unsupported_adapter"
  end
end

