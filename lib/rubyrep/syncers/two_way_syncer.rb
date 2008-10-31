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
    #   * :+update_left+: Update left database with the field values in the right db.
    #   * :+update_right+: Update right database with the field values in the left db.
    #   * +Proc+ object:
    #     If a Proc object is given, it is responsible for dealing with the
    #     record. Called with the following parameters:
    #     * sync_helper: The current SyncHelper instance.
    #     * type: always :+conflict+
    #     * rows: A two element array of rows (column_name => value hashes).
    #       First left, than right record.
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
          :sync_conflict_handling => :ignore
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

      # Verifies if the given :+conflict_handling+ option is valid.
      # Raises an ArgumentError if option is invalid
      def validate_conflict_handling_option(option)
        unless option.respond_to? :call
          unless [:ignore, :update_left, :update_right].include? option
            raise ArgumentError.new("#{option.inspect} not a valid :conflict_handling option")
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
        
        self.sync_helper = sync_helper
      end

      # Called to sync the provided difference.
      # See DirectTableScan#run for a description of the +type+ and +row+ parameters.
      def sync_difference(type, row)
        if type == :left or type == :right
          option_key = type == :left ? :left_record_handling : :right_record_handling
          option = sync_helper.sync_options[option_key]
          if option == :ignore
            # nothing to do
          elsif option == :delete
            sync_helper.delete_record type, row
          elsif option == :insert
            target = (type == :left ? :right : :left)
            sync_helper.insert_record target, row
          else #option must be a Proc
            option.call sync_helper, type, row
          end
        else
          option = sync_helper.sync_options[:sync_conflict_handling]
          if option == :ignore
            # nothing to do
          elsif option == :update_left
            sync_helper.update_record :left, row[1]
          elsif option == :update_right
            sync_helper.update_record :right, row[0]
          else #option must be a Proc
            option.call sync_helper, type, row
          end
        end
      end
    end
  end
end