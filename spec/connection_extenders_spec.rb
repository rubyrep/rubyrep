require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ConnectionExtenders do
  before(:each) do
  end

  it "extenders should return list of registered connection extenders" do
    ConnectionExtenders.extenders.include?(:postgresql).should be_true
  end
  
  it "register should register a new connection extender" do
    ConnectionExtenders.register(:bla => :blub)
    
    ConnectionExtenders.extenders.include?(:bla).should be_true
  end
  
  it "register should replace already existing connection extenders" do
    ConnectionExtenders.register(:bla => :blub)
    ConnectionExtenders.register(:bla => :blub2)
    
    ConnectionExtenders.extenders[:bla].should == :blub2
  end
end

