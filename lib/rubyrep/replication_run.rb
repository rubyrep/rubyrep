require 'timeout'

module RR
  
  # Executes a single replication run
  class ReplicationRun

    # The current Session object
    attr_accessor :session

    # The current TaskSweeper
    attr_accessor :sweeper

    # An array of ReplicationDifference which originally failed replication but should be tried one more time
    def second_chancers
      @second_chancers ||= []
    end

    # Returns the current ReplicationHelper; creates it if necessary
    def helper
      @helper ||= ReplicationHelper.new(self)
    end

    # Returns the current replicator; creates it if necessary.
    def replicator
      @replicator ||=
        Replicators.replicators[session.configuration.options[:replicator]].new(helper)
    end

    # Calls the event filter for the give  difference.
    # * +diff+: instance of ReplicationDifference
    # Returns +true+ if replication of the difference should *not* proceed.
    def event_filtered?(diff)
      event_filter = helper.options_for_table(diff.changes[:left].table)[:event_filter]
      if event_filter && event_filter.respond_to?(:before_replicate)
        not event_filter.before_replicate(
          diff.changes[:left].table,
          helper.type_cast(diff.changes[:left].table, diff.changes[:left].key),
          helper, 
          diff
        )
      else
        false
      end
    end

    # Returns the next available ReplicationDifference.
    # (Either new unprocessed differences or if not available, the first available 'second chancer'.)
    #
    def load_difference
      @loaders ||= LoggedChangeLoaders.new(session)
      @loaders.update # ensure the cache of change log records is up-to-date
      diff = ReplicationDifference.new @loaders
      diff.load
      unless diff.loaded? or second_chancers.empty?
        diff = second_chancers.shift
      end
      diff
    end

    # Executes the replication run.
    def run
      return unless [:left, :right].any? do |database|
        changes_pending = false
        t = Thread.new do
          changes_pending = session.send(database).select_one(
            "select id from #{session.configuration.options[:rep_prefix]}_pending_changes limit 1"
          ) != nil
        end
        t.join session.configuration.options[:database_connection_timeout]
        changes_pending
      end

      # Apparently sometimes above check for changes takes already so long, that
      # the replication run times out.
      # Check for this and if timed out, return (silently).
      return if sweeper.terminated?

      success = false
      begin
        replicator # ensure that replicator is created and has chance to validate settings

        loop do
          begin
            diff = load_difference
            break unless diff.loaded?
            break if sweeper.terminated?
            if diff.type != :no_diff and not event_filtered?(diff)
              replicator.replicate_difference diff
            end
          rescue Exception => e
            if e.message =~ /violates foreign key constraint|foreign key constraint fails/i and !diff.second_chance?
              # Note:
              # Identifying the foreign key constraint violation via regular expression is
              # database dependent and *dirty*.
              # It would be better to use the ActiveRecord #translate_exception mechanism.
              # However as per version 3.0.5 this doesn't work yet properly.

              diff.second_chance = true
              second_chancers << diff
            else
              begin
                helper.log_replication_outcome diff, e.message,
                  e.class.to_s + "\n" + e.backtrace.join("\n")
              rescue Exception => _
                # if logging to database itself fails, re-raise the original exception
                raise e
              end
            end
          end
        end
        success = true
      ensure
        if sweeper.terminated?
          helper.finalize false
          session.disconnect_databases
        else
          helper.finalize success
        end
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
