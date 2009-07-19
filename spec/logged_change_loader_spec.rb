require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe LoggedChangeLoaders do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initializers should create both logged change loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new(session)
    loaders[:left].session.should == session
    loaders[:left].database.should == :left
    loaders[:right].database.should == :right
  end

  it "update should execute a forced update of both logged change loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new(session)
    loaders[:left].should_receive(:update).with(:forced => true)
    loaders[:right].should_receive(:update).with(:forced => true)
    loaders.update
  end

end

describe LoggedChangeLoader do
  before(:each) do
    Initializer.configuration = standard_config
  end

  # Note:
  # LoggedChangeLoader is a helper for LoggedChange.
  # It is tested through the specs for LoggedChange.

  it "oldest_change_time should return nil if there are no changes" do
    session = Session.new
    session.left.execute "delete from rr_pending_changes"
    loader = LoggedChangeLoader.new session, :left
    loader.oldest_change_time.should be_nil
  end

  it "oldest_change_time should return the time of the oldest change" do
    session = Session.new
    session.left.begin_db_transaction
    begin
      time = Time.now
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => time
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => 100.seconds.from_now
      }
      loader = LoggedChangeLoader.new session, :left
      loader.oldest_change_time.should.to_s == time.to_s
    ensure
      session.left.rollback_db_transaction
    end
  end

end
