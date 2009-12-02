module RR
  module Replicators
    # This replicator implements a two way replication.
    # Options:
    # * :+left_change_handling+, :+right_change_handling+:
    #   Handling of records that were changed only in the named database.
    #   Can be any of the following:
    #   * :+ignore+: No action.
    #   * :+replicate+: Updates other database accordingly. *Default* *Setting*
    #   * +Proc+ object:
    #     If a Proc object is given, it is responsible for dealing with the
    #     record. Called with the following parameters:
    #     * replication_helper: The current ReplicationHelper instance.
    #     * difference: A ReplicationDifference instance describing the change
    # * :+replication_conflict_handling+:
    #   Handling of conflicting record changes. Can be any of the following:
    #   * :+ignore+: No action. *Default* *Setting*
    #   * :+left_wins+: The right database is updated accordingly.
    #   * :+right_wins+: The left database is updated accordingly.
    #   * :+later_wins+:
    #     The more recent change is replicated.
    #     (If both changes have same age: left change is replicated)
    #   * :+earlier_wins+:
    #     The less recent change is replicated.
    #     (If both records have same age: left change is replicated)
    #   * +Proc+ object:
    #     If a Proc object is given, it is responsible for dealing with the
    #     record. Called with the following parameters:
    #     * replication_helper: The current ReplicationHelper instance.
    #     * difference: A ReplicationDifference instance describing the changes
    # * :+logged_replication_events+:
    #   Specifies which types of replications are logged.
    #   Is either a single value or an array of multiple ones.
    #   Default: [:ignored_conflicts]
    #   Possible values:
    #   * :+ignored_changes+: log ignored (but not replicated) non-conflict changes
    #   * :+all_changes+: log all non-conflict changes
    #   * :+ignored_conflicts+: log ignored (but not replicated) conflicts
    #   * :+all_conflicts+: log all conflicts
    #
    # Example of using a Proc object for custom behaviour:
    #   lambda do |rep_helper, diff|
    #     # if specified as replication_conflict_handling option, logs all
    #     # conflicts to a text file
    #     File.open('/var/log/rubyrep_conflict_log', 'a') do |f|
    #       f.puts <<-end_str
    #         #{Time.now}: conflict
    #         * in table #{diff.changes[:left].table}
    #         * for record '#{diff.changes[:left].key}'
    #         * change type in left db: '#{diff.changes[:left].type}'
    #         * change type in right db: '#{diff.changes[:right].type}'
    #       end_str
    #     end
    #   end
    class TwoWayReplicator
      
      # Register the syncer
      Replicators.register :two_way => self

      # The current ReplicationHelper object
      attr_accessor :rep_helper

      # Provides default option for the replicator. Optional.
      # Returns a hash with key => value pairs.
      def self.default_options
        {
          :left_change_handling => :replicate,
          :right_change_handling => :replicate,
          :replication_conflict_handling => :ignore,
          :logged_replication_events => [:ignored_conflicts],
        }
      end

      # Checks if an option is configured correctly. Raises an ArgumentError if not.
      # * +table_spec+: the table specification to which the option belongs. May be +nil+.
      # * +valid_option_values+: array of valid option values
      # * +option_key+: the key of the option that is to be checked
      # * +option_value+: the value of the option that is to be checked
      def verify_option(table_spec, valid_option_values, option_key, option_value)
        unless valid_option_values.include? option_value
          message = ""
          message << "#{table_spec.inspect}: " if table_spec
          message << "#{option_value.inspect} not a valid #{option_key.inspect} option"
          raise ArgumentError.new(message)
        end
      end

      # Verifies if the :+left_change_handling+ / :+right_change_handling+
      # options are valid.
      # Raises an ArgumentError if an option is invalid
      def validate_change_handling_options
        [:left_change_handling, :right_change_handling].each do |key|
          rep_helper.session.configuration.each_matching_option(key) do |table_spec, value|
            unless value.respond_to? :call
              verify_option table_spec, [:ignore, :replicate], key, value
            end
          end
        end
      end

      # Verifies if the given :+replication_conflict_handling+ options are valid.
      # Raises an ArgumentError if an option is invalid.
      def validate_conflict_handling_options
        rep_helper.session.configuration.each_matching_option(:replication_conflict_handling) do |table_spec, value|
          unless value.respond_to? :call
            verify_option table_spec,
              [:ignore, :left_wins, :right_wins, :later_wins, :earlier_wins],
              :replication_conflict_handling, value
          end
        end
      end

      # Verifies if the given :+replication_logging+ option /options is / are valid.
      # Raises an ArgumentError if invalid
      def validate_logging_options
        rep_helper.session.configuration.each_matching_option(:logged_replication_events) do |table_spec, values|
          values = [values].flatten # ensure that I have an array
          values.each do |value|
            verify_option table_spec,
              [:ignored_changes, :all_changes, :ignored_conflicts, :all_conflicts],
              :logged_replication_events, value
          end
        end
      end

      # Initializes the TwoWayReplicator
      # Raises an ArgumentError if any of the replication options is invalid.
      #
      # Parameters:
      # * rep_helper:
      #   The ReplicationHelper object providing information and utility functions.
      def initialize(rep_helper)
        self.rep_helper = rep_helper

        validate_change_handling_options
        validate_conflict_handling_options
        validate_logging_options
      end

      # Shortcut to calculate the "other" database.
      OTHER_SIDE = {
        :left => :right,
        :right => :left
      }

      # Specifies how to clear conflicts.
      # The outer hash keys describe the type of the winning change.
      # The inner hash keys describe the type of the loosing change.
      # The inser hash values describe the action to take on the loosing side.
      CONFLICT_STATE_MATRIX = {
        :insert => {:insert => :update, :update => :update, :delete => :insert},
        :update => {:insert => :update, :update => :update, :delete => :insert},
        :delete => {:insert => :delete, :update => :delete, :delete => :delete}
      }

      # Helper function that clears a conflict by taking the change from the
      # specified winning database and updating the other database accordingly.
      # * +source_db+: the winning database (either :+left+ or :+right+)
      # * +diff+: the ReplicationDifference instance
      # * +remaining_attempts+: the number of remaining replication attempts for this difference
      def clear_conflict(source_db, diff, remaining_attempts)
        source_change = diff.changes[source_db]
        target_db = OTHER_SIDE[source_db]
        target_change = diff.changes[target_db]

        target_action = CONFLICT_STATE_MATRIX[source_change.type][target_change.type]
        source_key = source_change.type == :update ? source_change.new_key : source_change.key
        target_key = target_change.type == :update ? target_change.new_key : target_change.key
        case target_action
        when :insert
          attempt_insert source_db, diff, remaining_attempts, source_key
        when :update
          attempt_update source_db, diff, remaining_attempts, source_key, target_key
        when :delete
          attempt_delete source_db, diff, remaining_attempts, target_key
        end
      end

      # Logs replication of the specified difference as per configured
      # :+replication_conflict_logging+ / :+left_change_logging+ / :+right_change_logging+ options.
      # * +winner+: Either the winner database (:+left+ or :+right+) or :+ignore+
      # * +diff+: the ReplicationDifference instance
      def log_replication_outcome(winner, diff)
        options = rep_helper.options_for_table(diff.changes[:left].table)
        option_values = [options[:logged_replication_events]].flatten # make sure I have an array
        if diff.type == :conflict
          return unless option_values.include?(:all_conflicts) or option_values.include?(:ignored_conflicts)
          return if winner != :ignore and not option_values.include?(:all_conflicts)
          outcome = {:left => 'left_won', :right => 'right_won', :ignore => 'ignored'}[winner]
        else
          return unless option_values.include?(:all_changes) or option_values.include?(:ignored_changes)
          return if winner != :ignore and not option_values.include?(:all_changes)
          outcome = winner == :ignore ? 'ignored' : 'replicated'
        end
        rep_helper.log_replication_outcome diff, outcome
      end

      # How often a replication will be attempted (in case it fails because the
      # record in question was removed from the source or inserted into the
      # target database _after_ the ReplicationDifference was loaded
      MAX_REPLICATION_ATTEMPTS = 2

      # Attempts to read the specified record from the source database and insert
      # it into the target database.
      # Retries if insert fails due to missing source or suddenly existing target
      # record.
      # * +source_db+: either :+left+ or :+right+ - source database of replication
      # * +diff+: the current ReplicationDifference instance
      # * +remaining_attempts+: the number of remaining replication attempts for this difference
      # * +source_key+: a column_name => value hash identifying the source record
      def attempt_insert(source_db, diff, remaining_attempts, source_key)
        source_change = diff.changes[source_db]
        source_table = source_change.table
        target_db = OTHER_SIDE[source_db]
        target_table = rep_helper.corresponding_table(source_db, source_table)

        values = rep_helper.load_record source_db, source_table, source_key
        if values == nil
          diff.amend
          replicate_difference diff, remaining_attempts - 1, "source record for insert vanished"
        else
          attempt_change('insert', source_db, target_db, diff, remaining_attempts) do
            rep_helper.insert_record target_db, target_table, values
            log_replication_outcome source_db, diff
          end
        end
      end

      # Attempts to read the specified record from the source database and update
      # the specified record in the target database.
      # Retries if update fails due to missing source
      # * +source_db+: either :+left+ or :+right+ - source database of replication
      # * +diff+: the current ReplicationDifference instance
      # * +remaining_attempts+: the number of remaining replication attempts for this difference
      # * +source_key+: a column_name => value hash identifying the source record
      # * +target_key+: a column_name => value hash identifying the source record
      def attempt_update(source_db, diff, remaining_attempts, source_key, target_key)
        source_change = diff.changes[source_db]
        source_table = source_change.table
        target_db = OTHER_SIDE[source_db]
        target_table = rep_helper.corresponding_table(source_db, source_table)

        values = rep_helper.load_record source_db, source_table, source_key
        if values == nil
          diff.amend
          replicate_difference diff, remaining_attempts - 1, "source record for update vanished"
        else
          attempt_change('update', source_db, target_db, diff, remaining_attempts) do
            number_updated = rep_helper.update_record target_db, target_table, values, target_key
            if number_updated == 0
              diff.amend
              replicate_difference diff, remaining_attempts - 1, "target record for update vanished"
            else
              log_replication_outcome source_db, diff
            end
          end
        end
      end

      # Helper for execution of insert / update / delete attempts.
      # Wraps those attempts into savepoints and handles exceptions.
      #
      # Note:
      # Savepoints have to be used for PostgreSQL (as a failed SQL statement
      # will otherwise invalidate the complete transaction.)
      #
      # * +action+: short description of change (e. g.: "update" or "delete")
      # * +source_db+: either :+left+ or :+right+ - source database of replication
      # * +target_db+: either :+left+ or :+right+ - target database of replication
      # * +diff+: the current ReplicationDifference instance
      # * +remaining_attempts+: the number of remaining replication attempts for this difference
      def attempt_change(action, source_db, target_db, diff, remaining_attempts)
        begin
          rep_helper.session.send(target_db).execute "savepoint rr_#{action}_#{remaining_attempts}"
          yield
          unless rep_helper.new_transaction?
            rep_helper.session.send(target_db).execute "release savepoint rr_#{action}_#{remaining_attempts}"
          end
        rescue Exception => e
          rep_helper.session.send(target_db).execute "rollback to savepoint rr_#{action}_#{remaining_attempts}"
          diff.amend
          replicate_difference diff, remaining_attempts - 1,
            "#{action} failed with #{e.message}"
        end
      end

      # Attempts to delete the source record from the target database.
      # E. g. if +source_db is :+left+, then the record is deleted in database
      # :+right+.
      # * +source_db+: either :+left+ or :+right+ - source database of replication
      # * +diff+: the current ReplicationDifference instance
      # * +remaining_attempts+: the number of remaining replication attempts for this difference
      # * +target_key+: a column_name => value hash identifying the source record
      def attempt_delete(source_db, diff, remaining_attempts, target_key)
        change = diff.changes[source_db]
        target_db = OTHER_SIDE[source_db]
        target_table = rep_helper.corresponding_table(source_db, change.table)

        attempt_change('delete', source_db, target_db, diff, remaining_attempts) do
          number_updated = rep_helper.delete_record target_db, target_table, target_key
          if number_updated == 0
            diff.amend
            replicate_difference diff, remaining_attempts - 1, "target record for delete vanished"
          else
            log_replication_outcome source_db, diff
          end
        end
      end

      # Called to replicate the specified difference.
      # * :+diff+: ReplicationDifference instance
      # * :+remaining_attempts+: how many more times a replication will be attempted
      # * :+previous_failure_description+: why the previous replication attempt failed
      def replicate_difference(diff, remaining_attempts = MAX_REPLICATION_ATTEMPTS, previous_failure_description = nil)
        raise Exception, previous_failure_description || "max replication attempts exceeded" if remaining_attempts == 0
        options = rep_helper.options_for_table(diff.changes[:left].table)
        if diff.type == :left or diff.type == :right
          key = diff.type == :left ? :left_change_handling : :right_change_handling
          option = options[key]

          if option == :ignore
            log_replication_outcome :ignore, diff
          elsif option == :replicate
            source_db = diff.type

            change = diff.changes[source_db]

            case change.type
            when :insert
              attempt_insert source_db, diff, remaining_attempts, change.key
            when :update
              attempt_update source_db, diff, remaining_attempts, change.new_key, change.key
            when :delete
              attempt_delete source_db, diff, remaining_attempts, change.key
            end
          else # option must be a Proc
            option.call rep_helper, diff
          end
        elsif diff.type == :conflict
          option = options[:replication_conflict_handling]
          if option == :ignore
            log_replication_outcome :ignore, diff
          elsif option == :left_wins
            clear_conflict :left, diff, remaining_attempts
          elsif option == :right_wins
            clear_conflict :right, diff, remaining_attempts
          elsif option == :later_wins
            winner_db = diff.changes[:left].last_changed_at >= diff.changes[:right].last_changed_at ? :left : :right
            clear_conflict winner_db, diff, remaining_attempts
          elsif option == :earlier_wins
            winner_db = diff.changes[:left].last_changed_at <= diff.changes[:right].last_changed_at ? :left : :right
            clear_conflict winner_db, diff, remaining_attempts
          else # option must be a Proc
            option.call rep_helper, diff
          end
        end
      end
      
    end
  end
end