module RR
  # Replicators are classes that implement the replication policies.
  # This module provides functionality to register replicators and access the
  # list of registered replicators.
  # Each Replicator must register itself with Replicators#register.
  # Each Replicator must implement the following methods:
  #
  #   # Creates a new replicator (A replicator is used for one replication run only)
  #   #   * sync_helper: a SyncHelper object providing necessary information and functionalities
  #   def initialize(sync_helper)
  #
  #   # Called to sync the provided difference.
  #   # +difference+ is an instance of +ReplicationDifference+
  #   def replicate_difference(difference)
  #
  #   # Provides default option for the replicator. Optional.
  #   # Returns a hash with :key => value pairs.
  #   def self.default_options
  module Replicators
    # Returns a Hash of currently registered replicators.
    # (Empty Hash if no replicators were defined.)
    def self.replicators
      @replicators ||= {}
      @replicators
    end

    # Returns the correct replicator class as per provided options hash
    def self.configured_replicator(options)
      replicators[options[:replicator]]
    end

    # Registers one or multiple replicators.
    # syncer_hash is a Hash with
    #   key::   The adapter symbol as used to reference the replicator
    #   value:: The class implementing the replicator
    def self.register(replicator_hash)
      @replicators ||= {}
      @replicators.merge! replicator_hash
    end

  end
end