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
      if RUBY_PLATFORM =~ /java/
        adapter = config[:adapter]
        
        # As recommended in the activerecord-jdbc-adapter use the jdbc versions
        # of the Adapters. E. g. instead of "postgresql", "jdbcpostgresql".
        # However the activerecord-jdbcmysql-adapter (version 0.6) failed the 
        # multi-lingual test. So for mysql I am not rewriting the adapter name so
        # that I am using the activerecord built-in adapter (which passes the test)
        adapter = 'jdbc' + adapter unless adapter =~ /^jdbc/ or adapter == 'mysql'

        DummyActiveRecord.establish_connection(config.merge(:adapter => adapter))
      else
        DummyActiveRecord.establish_connection(config)
      end
      connection = DummyActiveRecord.connection

      # Delete the database connection from ActiveRecords's 'memory'
      ActiveRecord::Base.active_connections.delete DummyActiveRecord.name
      
      extender = ""
      if RUBY_PLATFORM =~ /java/ and config[:adapter] != 'mysql'
        # Also here: the standard mysql extender works perfectly fine under jruby.
        # So use it. For all other cases (under jruby) use the JDBC extender.
        extender = :jdbc
      elsif ConnectionExtenders.extenders.include? config[:adapter].to_sym
        extender = config[:adapter].to_sym
      else
        raise "No ConnectionExtender available for :#{config[:adapter]}"
      end
      mod = ConnectionExtenders.extenders[extender]
      connection.extend mod
      connection
    end
  end
end