require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationExtenders do
  before(:each) do
    Initializer.configuration = standard_config
    @@old_cache_status = ConnectionExtenders.use_db_connection_cache(false)
  end

  after(:each) do
    ConnectionExtenders.use_db_connection_cache(@@old_cache_status)
  end
  
  it "extenders should return list of registered connection extenders" do
    ReplicationExtenders.extenders.include?(:postgresql).should be_true
  end
  
  it "register should register a new connection extender" do
    ReplicationExtenders.register(:bla => :blub)
    
    ReplicationExtenders.extenders.include?(:bla).should be_true
  end
  
  it "register should replace already existing connection extenders" do
    ReplicationExtenders.register(:bla => :blub)
    ReplicationExtenders.register(:bla => :blub2)
    
    ReplicationExtenders.extenders[:bla].should == :blub2
  end
end

