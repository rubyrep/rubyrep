require 'timeout'

module RR
  
  # Executes a single replication run
  class ReplicationRun

    # The current Session object
    attr_accessor :session

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
            replicator.replicate_difference diff if diff.type != :no_diff
          rescue Exception => e
            helper.log_replication_outcome diff, e.message,
              e.class.to_s + "\n" + e.backtrace.join("\n")
          end
        end
        success = true # considered to be successful if we get till here
      ensure
        helper.finalize success
      end
    end

    # Creates a new ReplicationRun instance.
    # * +session+: the current Session
    def initialize(session)
      self.session = session
    end
  end
end
