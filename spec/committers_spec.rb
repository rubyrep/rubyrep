require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Committers do
  before(:each) do
    @old_committers = Committers.committers
  end

  after(:each) do
    Committers.instance_variable_set :@committers, @old_committers
  end
  
  it "committers should return empty hash if nil" do
    Committers.instance_variable_set :@committers, nil
    Committers.committers.should == {}
  end
  
  it "committers should return the registered committers" do
    Committers.instance_variable_set :@committers, :dummy_data
    Committers.committers.should == :dummy_data
  end
  
  it "register should register the provided commiter" do
    Committers.instance_variable_set :@committers, nil
    Committers.register :a_key => :a
    Committers.register :b_key => :b
    Committers.committers[:a_key].should == :a
    Committers.committers[:b_key].should == :b
  end
end


describe "Committer", :shared => true do
  it "execute_change should yield" do
    block_called = false
    @committer.notify_change([:left]) {block_called = true}
    block_called.should be_true
  end
  
  it "execute_change should raise if parameters contains invalid entries" do
    lambda {@committer.notify_change([:left, :right]) {}}.should_not raise_error
    lambda {@committer.notify_change([:left, :blub]) {}}.should raise_error(ArgumentError)
  end
  
  it "should support table_sync_completed" do
    @committer.table_sync_completed
  end
end

describe Committers::DefaultCommitter do
  before(:each) do
    @committer = Committers::DefaultCommitter.new :dummy_session, 
      "left", "right", :dummy_options
  end

  it "should register itself" do
    Committers.committers[:default].should == Committers::DefaultCommitter
  end
  
  it "initialize should store the provided parameters" do
    @committer.session.should == :dummy_session
    @committer.left_table.should == "left"
    @committer.right_table.should == "right"
    @committer.sync_options.should == :dummy_options
  end
  
  it_should_behave_like "Committer"
end

describe Committers::NeverCommitter do
  before(:each) do
    @old_session = Committers::NeverCommitter.current_session
    Committers::NeverCommitter.current_session = nil
    @session = mock("session", :null_object => true)
    @committer = Committers::NeverCommitter.new @session,
      "left", "right", :dummy_options
  end
  
  after(:each) do
    Committers::NeverCommitter.current_session = @old_session
  end

  it "should register itself" do
    Committers.committers[:never_commit].should == Committers::NeverCommitter
  end
  
  it "initialize should store the provided parameters" do
    @committer.session.should == @session
    @committer.left_table.should == "left"
    @committer.right_table.should == "right"
    @committer.sync_options.should == :dummy_options
  end
  
  it "initialize should rollback the previous current sesson and then register the new one as current session" do
    old_session = mock("old session", :null_object => true)
    new_session = mock("new session", :null_object => true)
    Committers::NeverCommitter.current_session = old_session
    Committers::NeverCommitter.should_receive(:rollback_current_session)
    
    Committers::NeverCommitter.new new_session, "left", "right", :dummy_options
    Committers::NeverCommitter.current_session.should == new_session
  end
  
  it "initialize should start new transactions" do
    # Ensure that initialize handles the case of no previous database session 
    # being present
    Committers::NeverCommitter.current_session = nil
    new_session = mock("new session")

    left_connection = mock("connection")
    left_connection.should_receive :begin_db_transaction
    new_session.should_receive(:left).and_return(left_connection)

    right_connection = mock("connection")
    right_connection.should_receive :begin_db_transaction
    new_session.should_receive(:right).and_return(right_connection)

    @committer = Committers::NeverCommitter.new new_session,
      "left", "right", :dummy_options
  end
  
  it "rollback_current_session should rollback current session" do
    old_session = mock("old session")

    left_connection = mock("connection")
    left_connection.should_receive :rollback_db_transaction
    old_session.should_receive(:left).and_return(left_connection)

    right_connection = mock("connection")
    right_connection.should_receive :rollback_db_transaction
    old_session.should_receive(:right).and_return(right_connection)
    
    Committers::NeverCommitter.current_session = old_session
    Committers::NeverCommitter.rollback_current_session
  end
  
  it "should work will real sessions" do
    session = Session.new(standard_config)
    Committers::NeverCommitter.new session, "left", "right", :dummy_options    
    Committers::NeverCommitter.new session, "left", "right", :dummy_options
    Committers::NeverCommitter.rollback_current_session
  end
  
  it "should work will real proxied sessions" do
    ensure_proxy
    session = Session.new(proxied_config)
    Committers::NeverCommitter.new session, "left", "right", :dummy_options    
    Committers::NeverCommitter.new session, "left", "right", :dummy_options   
    Committers::NeverCommitter.rollback_current_session
  end
  
  it_should_behave_like "Committer"
  
end