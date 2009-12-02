module RR
  # Syncers are classes that implement the sync policies.
  # This module provides functionality to register syncers and access the
  # list of registered syncers.
  # Each Syncer must register itself with Syncers#register.
  # Each Syncer must implement the following methods:
  #
  #   # Creates a new syncer (A syncer is used for one table sync only)
  #   #   * sync_helper: a SyncHelper object providing necessary information and functionalities
  #   def initialize(sync_helper)
  #
  #   # Called to sync the provided difference.
  #   # See DirectTableScan#run for a description of the +type+ and +row+ parameters.
  #   def sync_difference(type, row)
  #
  #   # Provides default option for the syncer. Optional.
  #   # Returns a hash with :key => value pairs.
  #   def self.default_options
  module Syncers
    # Returns a Hash of currently registered syncers.
    # (Empty Hash if no syncers were defined.)
    def self.syncers
      @syncers ||= {}
      @syncers
    end
  
    # Registers one or multiple syncers.
    # syncer_hash is a Hash with
    #   key::   The adapter symbol as used to reference the syncer
    #   value:: The class implementing the syncer
    def self.register(syncer_hash)
      @syncers ||= {}
      @syncers.merge! syncer_hash
    end

    # Returns the correct syncer class as per provided options hash
    def self.configured_syncer(options)
      syncer_id = options[:syncer]
      syncer_id ||= options[:replicator]
      syncers[syncer_id]
    end
    
    # This syncer implements a one way sync.
    # Syncer options relevant for this syncer:
    #   * +:direction+: Sync direction. Possible values:
    #     * +:left+
    #     * +:right+
    #   * +:delete+: Default: false. If true, deletes in the target database all
    #                records _not_ existing in the source database.
    #   * +:update+: If true (default), update records in the target database
    #                if different.
    #   * +:insert+: If true (default), copy over records not existing in the
    #                target database.
    class OneWaySyncer
      
      # Register the syncer
      Syncers.register :one_way => self

      # The current SyncHelper object
      attr_accessor :sync_helper

      # ID of source database (either :left or :right)
      attr_accessor :source

      # ID of target database (either :left or :right)
      attr_accessor :target

      # Array index to source row in case #sync_difference +type+ is :conflict.
      # (As in that case the +row+ parameter is an array of left and right records.)
      attr_accessor :source_record_index
      
      # Provides default option for the syncer. Optional.
      # Returns a hash with :key => value pairs.
      def self.default_options
        {
          :direction => :right,
          :delete => false, :update => true, :insert => true
        }
      end

      # Initializes the syncer
      #   * sync_helper: The SyncHelper object provided information and utility
      #                  functions.
      def initialize(sync_helper)
        self.sync_helper = sync_helper
        self.source = sync_helper.sync_options[:direction] == :left ? :right : :left
        self.target = sync_helper.sync_options[:direction] == :left ? :left : :right
        self.source_record_index = sync_helper.sync_options[:direction] == :left ? 1 : 0
      end

      # Called to sync the provided difference.
      # See DirectTableScan#run for a description of the +type+ and +row+ parameters.
      def sync_difference(type, row)
        case type
        when source
          if sync_helper.sync_options[:insert]
            sync_helper.insert_record target, sync_helper.tables[target], row
          end
        when target
          if sync_helper.sync_options[:delete]
            sync_helper.delete_record target, sync_helper.tables[target], row
          end
        when :conflict
          if sync_helper.sync_options[:update]
            sync_helper.update_record target, sync_helper.tables[target], row[source_record_index]
          end
        end
      end
    end
    
  end
end