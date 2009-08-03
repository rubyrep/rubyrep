require 'timeout'

module RR
  
  # Executes a single replication run
  class ReplicationRun

    # The current Session object
    attr_accessor :session

    # The current TaskSweeper
    attr_accessor :sweeper

    # Returns the current ReplicationHelper; creates it if necessary
    def helper
      @helper ||= ReplicationHelper.new(self)
    end

    # Returns the current replicator; creates it if necessary.
    def replicator
      @replicator ||=
        Replicators.replicators[session.configuration.options[:replicator]].new(helper)
    end

    # Executes the replication run.
    def run
      return unless [:left, :right].any? do |database|
        changes_pending = false
        t = Thread.new do
          changes_pending = session.send(database).select_one(
            "select id from #{session.configuration.options[:rep_prefix]}_pending_changes"
          ) != nil
        end
        t.join session.configuration.options[:database_connection_timeout]
        changes_pending
      end

      # Apparently sometimes above check for changes takes already so long, that
      # the replication run times out.
      # Check for this and if timed out, return (silently).
      return if sweeper.terminated?

      loaders = LoggedChangeLoaders.new(session)

      begin
        success = false
        replicator # ensure that replicator is created and has chance to validate settings

        loop do
          begin
            loaders.update # ensure the cache of change log records is up-to-date
            diff = ReplicationDifference.new loaders
            diff.load
            break unless diff.loaded?
            break if sweeper.terminated?
            replicator.replicate_difference diff if diff.type != :no_diff
          rescue Exception => e
            begin
              helper.log_replication_outcome diff, e.message,
                e.class.to_s + "\n" + e.backtrace.join("\n")
            rescue Exception => _
              # if logging to database itself fails, re-raise the original exception
              raise e
            end
          end
        end
        # considered to be successful if we get till here without timing out
        success = true unless sweeper.terminated?
      ensure
        helper.finalize success
      end
    end

    # Installs the current sweeper into the database connections
    def install_sweeper
      [:left, :right].each do |database|
        unless session.send(database).respond_to?(:sweeper)
          session.send(database).send(:extend, NoisyConnection)
        end
        session.send(database).sweeper = sweeper
      end
    end

    # Creates a new ReplicationRun instance.
    # * +session+: the current Session
    # * +sweeper+: the current TaskSweeper
    def initialize(session, sweeper)
      self.session = session
      self.sweeper = sweeper
      install_sweeper
    end
  end
end
