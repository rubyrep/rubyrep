require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe NoisyConnection do
  before(:each) do
    Initializer.configuration = proxied_config
    @connection = ProxyConnection.new Initializer.configuration.left
    @connection.send(:extend, NoisyConnection)
    @connection.sweeper = TaskSweeper.new(1)
  end

  it "select_cursor should return correct results" do
    @connection.sweeper.should_receive(:ping).exactly(4).times
    @connection.select_record(:table => 'scanner_records').should == {
      'id' => 1,
      'name' => 'Alice - exists in both databases'
    }
  end

  it "insert_record should write nil values correctly" do
    @connection.sweeper.should_receive(:ping).exactly(2).times
    @connection.begin_db_transaction
    begin
      @connection.insert_record('extender_combined_key', 'first_id' => 8, 'second_id' => '9', 'name' => nil)
      @connection.select_one(
        "select name from extender_combined_key where (first_id, second_id) = (8, 9)"
      ).should == {"name" => nil}
    ensure
      @connection.rollback_db_transaction
    end
  end

  it "update_record should update the specified record" do
    @connection.sweeper.should_receive(:ping).exactly(2).times
    @connection.begin_db_transaction
    begin
      @connection.update_record('scanner_records', 'id' => 1, 'name' => 'update_test')
      @connection.select_one(
        "select name from scanner_records where id = 1"
      ).should == {'name' => 'update_test'}
    ensure
      @connection.rollback_db_transaction
    end
  end

  it "delete_record should delete the specified record" do
    @connection.sweeper.should_receive(:ping).exactly(2).times
    @connection.begin_db_transaction
    begin
      @connection.delete_record('extender_combined_key', 'first_id' => 1, 'second_id' => '1', 'name' => 'xy')
      @connection.select_one(
        "select first_id, second_id, name
         from extender_combined_key where (first_id, second_id) = (1, 1)") \
        .should be_nil
    ensure
      @connection.rollback_db_transaction
    end
  end

  it "commit_db_transaction should update TaskSweeper" do
    @connection.begin_db_transaction
    initializer = ReplicationInitializer.new Session.new(standard_config)
    begin
      @connection.execute "insert into scanner_records(id,name) values(99, 'bla')"
      @connection.sweeper.should_receive(:ping).exactly(2).times
      @connection.commit_db_transaction
      initializer.silence_ddl_notices(:left) do # avoid PostgreSQL warning that no transaction is open
        @connection.rollback_db_transaction
      end
      @connection.select_one("select name from scanner_records where id = 99")['name'].
        should == 'bla'
    ensure
      @connection.execute "delete from scanner_records where id = 99"
    end
  end

end