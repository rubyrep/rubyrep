module RR
  module Syncers
    # This syncer implements a two way sync.
    # Syncer options relevant for this syncer:
    # * :+left_record_handling+, :+right_record_handling+:
    #   Handling of records only existing only in the named database.
    #   Can be any of the following:
    #   * :+ignore+: No action.
    #   * :+delete+: Delete from the source database.
    #   * :+insert+: Insert in the target database. *Default* *Setting*
    #   * +Proc+ object:
    #     If a Proc object is given, it is responsible for dealing with the
    #     record. Called with the following parameters:
    #     * sync_helper: The current SyncHelper instance.
    #     * type: :+left+ or :+right+ to designate source database
    #     * row: column_name => value hash representing the row
    # * :+sync_conflict_handling+:
    #   Handling of conflicting records. Can be any of the following:
    #   * :+ignore+: No action. *Default* *Setting*
    #   * :+left_wins+: Update right database with the field values in the left db.
    #   * :+right_wins+: Update left database with the field values in the right db.
    #   * +Proc+ object:
    #     If a Proc object is given, it is responsible for dealing with the
    #     record. Called with the following parameters:
    #     * sync_helper: The current SyncHelper instance.
    #     * type: always :+conflict+
    #     * rows: A two element array of rows (column_name => value hashes).
    #       First left, than right record.
    # * :+logged_sync_events+:
    #   Specifies which types of syncs are logged.
    #   Is either a single value or an array of multiple ones.
    #   Default: [:ignored_conflicts]
    #   Possible values:
    #   * :+ignored_changes+: log ignored (but not synced) non-conflict changes
    #   * :+all_changes+: log all non-conflict changes
    #   * :+ignored_conflicts+: log ignored (but not synced) conflicts
    #   * :+all_conflicts+: log all conflicts
    #
    # Example of using a Proc object:
    #   lambda do |sync_helper, type, row|
    #     # delete records existing only in the left database.
    #     sync_helper.delete(type, row) if type == :left
    #   end
    class TwoWaySyncer
      
      # Register the syncer
      Syncers.register :two_way => self

      # The current SyncHelper object
      attr_accessor :sync_helper

      # Provides default option for the syncer. Optional.
      # Returns a hash with key => value pairs.
      def self.default_options
        {
          :left_record_handling => :insert,
          :right_record_handling => :insert,
          :sync_conflict_handling => :ignore,
          :logged_sync_events => [:ignored_conflicts]
        }
      end

      # Verifies if the given :+left_record_handling+ / :+right_record_handling+
      # option is valid.
      # Raises an ArgumentError if option is invalid
      def validate_left_right_record_handling_option(option)
        unless option.respond_to? :call
          unless [:ignore, :delete, :insert].include? option
            raise ArgumentError.new("#{option.inspect} not a valid :left_record_handling / :right_record_handling option")
          end
        end
      end

      # Verifies if the given :+sync_conflict_handling+ option is valid.
      # Raises an ArgumentError if option is invalid
      def validate_conflict_handling_option(option)
        unless option.respond_to? :call
          unless [:ignore, :right_wins, :left_wins].include? option
            raise ArgumentError.new("#{option.inspect} not a valid :sync_conflict_handling option")
          end
        end
      end

      # Verifies if the given :+replication_logging+ option /options is / are valid.
      # Raises an ArgumentError if invalid
      def validate_logging_options(options)
        values = [options].flatten # ensure that I have an array
        values.each do |value|
          unless [:ignored_changes, :all_changes, :ignored_conflicts, :all_conflicts].include? value
            raise ArgumentError.new("#{value.inspect} not a valid :logged_sync_events option")
          end
        end
      end

      # Initializes the syncer
      # * sync_helper:
      #   The SyncHelper object provided information and utility functions.
      # Raises an ArgumentError if any of the option in sync_helper.sync_options
      # is invalid.
      def initialize(sync_helper)
        validate_left_right_record_handling_option sync_helper.sync_options[:left_record_handling]
        validate_left_right_record_handling_option sync_helper.sync_options[:right_record_handling]
        validate_conflict_handling_option sync_helper.sync_options[:sync_conflict_handling]
        validate_logging_options sync_helper.sync_options[:logged_sync_events]
        
        self.sync_helper = sync_helper
      end

      # Sync type descriptions that are written into the event log
      TYPE_DESCRIPTIONS = {
        :left => 'left_record',
        :right => 'right_record',
        :conflict => 'conflict'
      }

      # Returns the :logged_sync_events option values.
      def log_option_values
        @log_option_values ||= [sync_helper.sync_options[:logged_sync_events]].flatten
      end
      private :log_option_values

      # Logs a sync event into the event log table as per configuration options.
      # * +type+: Refer to DirectTableScan#run for a description
      # * +action+: the sync action that is executed
      #   (The :+left_record_handling+ / :+right_record_handling+ or
      #   :+sync_conflict_handling+ option)
      # * +row+: Refer to DirectTableScan#run for a description
      def log_sync_outcome(type, action, row)
        if type == :conflict
          return unless log_option_values.include?(:all_conflicts) or log_option_values.include?(:ignored_conflicts)
          return if action != :ignore and not log_option_values.include?(:all_conflicts)
          row = row[0] # Extract left row from row array
        else
          return unless log_option_values.include?(:all_changes) or log_option_values.include?(:ignored_changes)
          return if action != :ignore and not log_option_values.include?(:all_changes)
        end

        sync_helper.log_sync_outcome row, TYPE_DESCRIPTIONS[type], action
      end

      # Called to sync the provided difference.
      # See DirectTableScan#run for a description of the +type+ and +row+ parameters.
      def sync_difference(type, row)
        if type == :left or type == :right
          option_key = type == :left ? :left_record_handling : :right_record_handling
          option = sync_helper.sync_options[option_key]
          log_sync_outcome type, option, row unless option.respond_to?(:call)
          if option == :ignore
            # nothing to do
          elsif option == :delete
            sync_helper.delete_record type, sync_helper.tables[type], row
          elsif option == :insert
            target = (type == :left ? :right : :left)
            sync_helper.insert_record target, sync_helper.tables[target], row
          else #option must be a Proc
            option.call sync_helper, type, row
          end
        else
          option = sync_helper.sync_options[:sync_conflict_handling]
          log_sync_outcome type, option, row unless option.respond_to?(:call)
          if option == :ignore
            # nothing to do
          elsif option == :right_wins
            sync_helper.update_record :left, sync_helper.tables[:left], row[1]
          elsif option == :left_wins
            sync_helper.update_record :right, sync_helper.tables[:right], row[0]
          else #option must be a Proc
            option.call sync_helper, type, row
          end
        end
      end
    end
  end
end