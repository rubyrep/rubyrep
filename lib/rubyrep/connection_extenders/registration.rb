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
    
    # Creates an ActiveRecord database connection according to the provided +config+ connection hash.
    # Possible values of this parameter are described in ActiveRecord::Base#establish_connection.
    # The database connection is extended with the correct ConnectionExtenders module.
    # 
    # ActiveRecord only allows one database connection per class.
    # (It disconnects the existing database connection if a new connection is established.)
    # To go around this, we delete ActiveRecord's memory of the existing database connection
    # as soon as it is created.
    def self.db_connect_without_cache(config)
      if RUBY_PLATFORM =~ /java/
        adapter = config[:adapter]
        
        # As recommended in the activerecord-jdbc-adapter use the jdbc versions
        # of the Adapters. E. g. instead of "postgresql", "jdbcpostgresql".
        adapter = 'jdbc' + adapter unless adapter =~ /^jdbc/

        DummyActiveRecord.establish_connection(config.merge(:adapter => adapter))
      else
        DummyActiveRecord.establish_connection(config)
      end
      connection = DummyActiveRecord.connection

      # Delete the database connection from ActiveRecords's 'memory'
      ActiveRecord::Base.active_connections.delete DummyActiveRecord.name
      
      extender = ""
      if RUBY_PLATFORM =~ /java/
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
    
    @@use_cache = true
    
    # Returns the current cache status (+true+ if caching is used; +false+ otherwise).
    def self.use_cache?; @@use_cache; end
    
    # Returns the connection cache hash.
    def self.connection_cache; @@connection_cache; end
    
    # Sets a new connection cache
    def self.connection_cache=(cache)
      @@connection_cache = cache
    end
    
    # Creates database connections by calling #db_connect_without_cache with the 
    # provided +config+ configuration hash.
    # A new database connection is created only if no according cached connection
    # is available.
    def self.db_connect(config)
      config_dump = Marshal.dump config.reject {|key, | [:proxy_host, :proxy_port].include? key}
      config_checksum = Digest::SHA1.hexdigest(config_dump)
      @@connection_cache ||= {}
      cached_db_connection = connection_cache[config_checksum]
      if use_cache? and cached_db_connection and cached_db_connection.active?
        cached_db_connection
      else
        db_connection = db_connect_without_cache config
        connection_cache[config_checksum] = db_connection if @@use_cache
        db_connection
      end
    end
    
    # If status == true: enable the cache. If status == false: don' use cache
    # Returns the old connection caching status
    def self.use_db_connection_cache(status)
      old_status, @@use_cache = @@use_cache, status
      old_status
    end
    
    # Free up all cached connections
    def self.clear_db_connection_cache
      @@connection_cache = {}
    end
  end
end