module RR

  # Replication extenders are modules that provide database specific functionality
  # required for replication. They are mixed into ActiveRecord database connections.
  # This module itself only provides functionality to register and retrieve
  # such extenders.
  module ReplicationExtenders
    # Returns a Hash of currently registered replication extenders.
    # (Empty Hash if no replication extenders were defined.)
    def self.extenders
      @extenders ||= {}
      @extenders
    end

    # Registers one or multiple replication extender.
    # extender is a Hash with
    #   key::   The adapter symbol as used by ActiveRecord::Connection Adapters, e. g. :postgresql
    #   value:: Name of the module implementing the replication extender
    def self.register(extender)
      @extenders ||= {}
      @extenders.merge! extender
    end
  end
end


