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

      # Default TwoWayReplicator options.
      DEFAULT_OPTIONS =  {
        :left_change_handling => :replicate,
        :right_change_handling => :replicate,
        :replication_conflict_handling => :ignore
      }

      # Returns the current options.
      def options
        @options ||= DEFAULT_OPTIONS.merge(rep_helper.options)
      end

      # Verifies if the given :+left_change_handling+ / :+right_change_handling+
      # option is valid.
      # Raises an ArgumentError if option is invalid
      def validate_left_right_change_handling_option(option)
        unless option.respond_to? :call
          unless [:ignore, :replicate].include? option
            raise ArgumentError.new("#{option.inspect} not a valid :left_change_handling / :right_change_handling option")
          end
        end
      end

      # Verifies if the given :+replication_conflict_handling+ option is valid.
      # Raises an ArgumentError if option is invalid
      def validate_conflict_handling_option(option)
        unless option.respond_to? :call
          unless [:ignore, :left_wins, :right_wins, :later_wins, :earlier_wins].include? option
            raise ArgumentError.new("#{option.inspect} not a valid :replication_conflict_handling option")
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

        validate_left_right_change_handling_option options[:left_change_handling]
        validate_left_right_change_handling_option options[:right_change_handling]
        validate_conflict_handling_option options[:replication_conflict_handling]
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
      # * diff: the ReplicationDifference instance
      def clear_conflict(source_db, diff)
        source_change = diff.changes[source_db]
        target_db = OTHER_SIDE[source_db]
        target_change = diff.changes[target_db]
        target_table = rep_helper.corresponding_table(source_db, source_change.table)

        target_action = CONFLICT_STATE_MATRIX[source_change.type][target_change.type]
        source_key = source_change.type == :update ? source_change.new_key : source_change.key
        target_key = target_change.type == :update ? target_change.new_key : target_change.key
        case target_action
        when :insert
          values = rep_helper.load_record source_db, source_change.table, source_key
          rep_helper.insert_record target_db, target_table, values
        when :update
          values = rep_helper.load_record source_db, source_change.table, source_key
          rep_helper.update_record target_db, target_table, values
        when :delete
          rep_helper.delete_record target_db, target_table, target_key
        end
      end

      # Called to replicate the specified difference.
      # * :+diff+: ReplicationDifference instance
      def replicate_difference(diff)
        if diff.type == :left or diff.type == :right
          key = diff.type == :left ? :left_change_handling : :right_change_handling
          option = options[key]

          if option == :ignore
            # nothing to do
          elsif option == :replicate
            source_db = diff.type
            target_db = OTHER_SIDE[source_db]

            change = diff.changes[source_db]
            target_table = rep_helper.corresponding_table(source_db, change.table)

            case change.type
            when :insert
              values = rep_helper.load_record source_db, change.table, change.key
              rep_helper.insert_record target_db, target_table, values
            when :update
              values = rep_helper.load_record source_db, change.table, change.new_key
              rep_helper.update_record target_db, target_table, values, change.key
            when :delete
              rep_helper.delete_record target_db, target_table, change.key
            end
          else # option must be a Proc
            option.call rep_helper, diff
          end
        else
          option = options[:replication_conflict_handling]
          if option == :ignore
            # nothing to do
          elsif option == :left_wins
            clear_conflict :left, diff
          elsif option == :right_wins
            clear_conflict :right, diff
          elsif option == :later_wins
            winner_db = diff.changes[:left].last_changed_at >= diff.changes[:right].last_changed_at ? :left : :right
            clear_conflict winner_db, diff
          elsif option == :earlier_wins
            winner_db = diff.changes[:left].last_changed_at <= diff.changes[:right].last_changed_at ? :left : :right
            clear_conflict winner_db, diff
          else # option must be a Proc
            option.call rep_helper, diff
          end
        end
      end
      
    end
  end
end