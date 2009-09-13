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
  it "should support the right constructor interface" do
    session = mock("session")
    session.should_receive(:left).any_number_of_times \
      .and_return(mock("left connection", :null_object => true))
    session.should_receive(:right).any_number_of_times \
      .and_return(mock("right connection", :null_object => true))
    @committer.class.new session
  end
  
  it "should proxy insert_record, update_record and delete_record calls" do
    left_connection = mock("left connection", :null_object => true)
    left_connection.should_receive(:insert_record).with("left", :dummy_insert_values)

    right_connection = mock("right connection", :null_object => true)
    right_connection.should_receive(:update_record).with("right", :dummy_update_values, :dummy_org_key)
    right_connection.should_receive(:delete_record).with("right", :dummy_delete_values)

    session = mock("session")
    session.should_receive(:left).any_number_of_times.and_return(left_connection)
    session.should_receive(:right).any_number_of_times.and_return(right_connection)

    committer = @committer.class.new session

    committer.insert_record :left, 'left', :dummy_insert_values
    committer.update_record :right, 'right', :dummy_update_values, :dummy_org_key
    committer.delete_record :right, 'right', :dummy_delete_values
  end
  
  it "should support finalize" do
    @committer.finalize(false)
  end
end

describe Committers::DefaultCommitter do
  before(:each) do
    @session = mock("session")
    @session.should_receive(:left).any_number_of_times.and_return(:left_connection)
    @session.should_receive(:right).any_number_of_times.and_return(:right_connection)
    @committer = Committers::DefaultCommitter.new @session
  end

  it "should register itself" do
    Committers.committers[:default].should == Committers::DefaultCommitter
  end
  
  it "initialize should store the provided parameters" do
    @committer.session.should == @session
    @committer.connections \
      .should == {:left => @session.left, :right => @session.right}
  end

  it "new_transaction? should return false" do
    @committer.new_transaction?.should be_false
  end
  
  it_should_behave_like "Committer"
end

describe Committers::NeverCommitter do
  before(:each) do
    @old_session = Committers::NeverCommitter.current_session
    Committers::NeverCommitter.current_session = nil
    @session = mock("session")
    @session.should_receive(:left).any_number_of_times \
      .and_return(mock("left connection", :null_object => true))
    @session.should_receive(:right).any_number_of_times \
      .and_return(mock("right connection", :null_object => true))
    @committer = Committers::NeverCommitter.new @session
  end
  
  after(:each) do
    Committers::NeverCommitter.current_session = @old_session
  end

  it "should register itself" do
    Committers.committers[:never_commit].should == Committers::NeverCommitter
  end
  
  it "initialize should store the provided parameters" do
    @committer.session.should == @session
    @committer.connections \
      .should == {:left => @session.left, :right => @session.right}
  end
  
  it "initialize should rollback the previous current session and then register the new one as current session" do
    old_session = mock("old session", :null_object => true)
    new_session = mock("new session", :null_object => true)
    Committers::NeverCommitter.current_session = old_session
    Committers::NeverCommitter.should_receive(:rollback_current_session)
    
    Committers::NeverCommitter.new new_session
    Committers::NeverCommitter.current_session.should == new_session
  end
  
  it "initialize should start new transactions" do
    # Ensure that initialize handles the case of no previous database session 
    # being present
    Committers::NeverCommitter.current_session = nil
    new_session = mock("new session")

    left_connection = mock("left connection")
    left_connection.should_receive :begin_db_transaction
    new_session.should_receive(:left).any_number_of_times.and_return(left_connection)

    right_connection = mock("right connection")
    right_connection.should_receive :begin_db_transaction
    new_session.should_receive(:right).any_number_of_times.and_return(right_connection)

    @committer = Committers::NeverCommitter.new new_session
  end
  
  it "rollback_current_session should rollback current session" do
    old_session = mock("old session")

    left_connection = mock("left connection")
    left_connection.should_receive :rollback_db_transaction
    old_session.should_receive(:left).and_return(left_connection)

    right_connection = mock("right connection")
    right_connection.should_receive :rollback_db_transaction
    old_session.should_receive(:right).and_return(right_connection)
    
    Committers::NeverCommitter.current_session = old_session
    Committers::NeverCommitter.rollback_current_session
  end
  
  it "should work will real sessions" do
    session = Session.new(standard_config)
    Committers::NeverCommitter.new session
    Committers::NeverCommitter.new session
    Committers::NeverCommitter.rollback_current_session
  end
  
  it "should work will real proxied sessions" do
    ensure_proxy
    session = Session.new(proxied_config)
    Committers::NeverCommitter.new session
    Committers::NeverCommitter.new session   
    Committers::NeverCommitter.rollback_current_session
  end
  
  it_should_behave_like "Committer"
  
end