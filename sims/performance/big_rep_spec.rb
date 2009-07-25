require 'benchmark'

require File.dirname(__FILE__) + '/../../spec/spec_helper.rb'
require File.dirname(__FILE__) + '/../sim_helper'

include RR

describe "Big Rep" do
  before(:each) do
  end

  # Calculates and returns the number of records of the specified table.
  # * +session+ current Session instance
  # * +database+: designates database, either :left or :right
  # * +table+: name of the table
  def record_quantity(session, database, table = 'big_rep')
    session.send(database).select_one( \
        "select count(id) as count from #{table}")['count'].to_i
  end

  # Calculates and returns the sum of the number fields of records.
  # * +session+: current Session instance
  # * +database+: designates database, either :left or :right
  def record_sum(session, database)
    session.send(database).select_one( \
        "select sum(number1) + sum(number2) + sum(number3) + sum(number4) as sum
         from big_rep")['sum'].to_f
  end

  # Returns the number of changes that remain to be replicated
  # * +session+: current Session instance
  def number_changes(session)
    record_quantity(session, :left, 'rr_pending_changes') +
      record_quantity(session, :right, 'rr_pending_changes')
  end

  # Runs a replication of the big_rep table.
  def run_rep
    config = deep_copy(Initializer.configuration)
    config.options = {
      :committer => :buffered_commit,
      :replication_conflict_handling => :later_wins,
    }

    session = Session.new config
    initializer = ReplicationInitializer.new session
    begin
      [:left, :right].each do |database|
        session.send(database).execute "insert into big_rep select * from big_rep_backup"
        session.send(database).execute "insert into rr_pending_changes select * from big_rep_pending_changes"
        initializer.create_trigger database, 'big_rep'
      end

      puts "\nReplicating (#{session.proxied? ? :proxied : :direct}) table big_rep (#{number_changes(session)} changes)"

      t = Thread.new do
        remaining_changes = number_changes(session)
        progress_bar = ScanProgressPrinters::ProgressBar.new(remaining_changes, session, 'big_rep', 'big_rep')
        while remaining_changes > 0
          sleep 1
          new_remaining_changes = number_changes(session)
          progress_bar.step remaining_changes - new_remaining_changes
          remaining_changes = new_remaining_changes
        end
      end

      run = ReplicationRun.new session, TaskSweeper.new(5)
      benchmark = Benchmark.measure { run.run }
      t.join 10
      puts "\n  time required: #{benchmark}"

      left_fingerprint = {
        :quantity => record_quantity(session, :left),
        :sum => record_sum(session, :left)
      }
      right_fingerprint = {
        :quantity => record_quantity(session, :right),
        :sum => record_sum(session, :right)
      }
      left_fingerprint.should == right_fingerprint
    ensure
      [:left, :right].each do |database|
        initializer.drop_trigger database, 'big_rep'
        session.send(database).execute "delete from big_rep"
        session.send(database).execute "delete from rr_pending_changes"
      end
    end
  end

  it "Direct Replication should replicate correctly" do
    Initializer.configuration = standard_config
    run_rep
  end

  it "Proxied Replication should replicate correctly" do
    ensure_proxy
    Initializer.configuration = proxied_config
    run_rep
  end
end