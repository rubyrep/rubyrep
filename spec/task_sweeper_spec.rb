require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TaskSweeper do
  before(:each) do
  end

  it "should execute the given task" do
    x = nil
    TaskSweeper.timeout(1) {|sweeper| x = 1}
    x.should == 1
  end

  it "should raise exceptions thrown by the task" do
    lambda {
      TaskSweeper.timeout(1) {raise "bla"}
    }.should raise_error("bla")
  end

  it "should return if task stalls" do
    start = Time.now
    TaskSweeper.timeout(0.01) {sleep 10}.should be_terminated
    (Time.now - start < 5).should be_true
  end

  it "should not return if task is active" do
    start = Time.now
    TaskSweeper.timeout(0.1) do |sweeper|
      10.times do
        sleep 0.05
        sweeper.ping
      end
    end.should_not be_terminated
    (Time.now - start > 0.4).should be_true

  end

  it "should notify a stalled task about it's termination" do
    terminated = false
    TaskSweeper.timeout(0.01) do |sweeper|
      sleep 0.05
      terminated = sweeper.terminated?
    end.join
    terminated.should be_true
  end
end