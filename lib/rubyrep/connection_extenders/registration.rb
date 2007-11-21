module RR
  
  # Connection extenders provide additional database specific functionality
  # not coming in the ActiveRecord library.
  # This module itself only provides functionality to register and retrieve
  # such connection extenders.
  module ConnectionExtenders
    # Returns a Hash of currently registered connection extenders.
    # (Empty Hash if no connection extenders were defined.)
    def self.extenders
      @extenders ||= {}
      @extenders
    end
  
    # Registers one or multiple connection extender.
    # extender is a Hash with 
    #   key::   The adapter symbol as used by ActiveRecord::Connection Adapters, e. g. :postgresql
    #   value:: Name of the module implementing the connection extender
    def self.register(extender)
      @extenders ||= {}
      @extenders.merge! extender
    end

    # Dummy ActiveRecord descendant only used to create database connections.
    class DummyActiveRecord < ActiveRecord::Base
    end
    
    # Creates an ActiveRecord database connection according to the provided connection hash.
    # The database connection is extended with the correct ConnectionExtenders module.
    # 
    # ActiveRecord only allows one database connection per class.
    # (It disconnects the existing database connection if a new connection is established.)
    # To go around this, we delete ActiveRecord's memory of the existing database connection
    # as soon as it is created.
    def self.db_connect(config)
      DummyActiveRecord.establish_connection(config)
      connection = DummyActiveRecord.connection

      # Delete the database connection from ActiveRecords's 'memory'
      ActiveRecord::Base.active_connections.delete DummyActiveRecord.name
      
      unless ConnectionExtenders.extenders.include? config[:adapter].to_sym
        raise "No ConnectionExtender available for :#{config[:adapter]}"
      end
      mod = ConnectionExtenders.extenders[config[:adapter].to_sym]
      connection.extend mod
      connection
    end
  end
end