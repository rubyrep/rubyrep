class ActiveRecord::ConnectionAdapters::AbstractAdapter
  # The current log subscriber
  attr_accessor :log_subscriber
end

class ActiveRecord::ConnectionAdapters::Column
  # Bug in ActiveRecord parsing of PostgreSQL timestamps with microseconds:
  # Certain values are incorrectly rounded, thus ending up with timestamps
  # that are off by one microsecond.
  # This monkey patch fixes the problem.
  def self.fast_string_to_time(string)
    if string =~ Format::ISO_DATETIME
      microsec = ($7.to_f * 1_000_000).round # used to be #to_i instead
      new_time $1.to_i, $2.to_i, $3.to_i, $4.to_i, $5.to_i, $6.to_i, microsec
    end
  end
end

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
      ActiveRecord::Base.connection_handler.connection_pools.delete DummyActiveRecord.name
      
      extender = ""
      if RUBY_PLATFORM =~ /java/
        extender = :jdbc
      elsif ConnectionExtenders.extenders.include? config[:adapter].to_sym
        extender = config[:adapter].to_sym
      else
        raise "No ConnectionExtender available for :#{config[:adapter]}"
      end
      connection.extend ConnectionExtenders.extenders[extender]
      
      # Hack to get Postgres schema support under JRuby to par with the standard
      # ruby version
      if RUBY_PLATFORM =~ /java/ and config[:adapter].to_sym == :postgresql
        connection.extend RR::ConnectionExtenders::PostgreSQLExtender
        connection.initialize_search_path
      end

      replication_module = ReplicationExtenders.extenders[config[:adapter].to_sym]
      connection.extend replication_module if replication_module
      
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

    # Installs the configured logger (if any) into the database connection.
    # * +db_connection+: database connection (as produced by #db_connect)
    # * +config+: database configuration (as provided to #db_connect)
    def self.install_logger(db_connection, config)
      if config[:logger]
        if config[:logger].respond_to?(:debug)
          logger = config[:logger]
        else
          logger = ActiveSupport::BufferedLogger.new(config[:logger])
        end
        db_connection.instance_variable_set :@logger, logger
        if ActiveSupport.const_defined?(:Notifications)
          connection_object_id = db_connection.object_id
          db_connection.log_subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |name, start, finish, id, payload|
            if payload[:connection_id] == connection_object_id and logger.debug?
              logger.debug payload[:sql].squeeze(" ")
            end
          end
        end
      end
    end
    
    # Creates database connections by calling #db_connect_without_cache with the 
    # provided +config+ configuration hash.
    # A new database connection is created only if no according cached connection
    # is available.
    def self.db_connect(config)
      if not use_cache?
        db_connection = db_connect_without_cache config
      else
        config_dump = Marshal.dump config.reject {|key, | [:proxy_host, :proxy_port, :logger].include? key}
        config_checksum = Digest::SHA1.hexdigest(config_dump)
        @@connection_cache ||= {}

        db_connection = connection_cache[config_checksum]
        unless db_connection and db_connection.active?
          db_connection = db_connect_without_cache config
          connection_cache[config_checksum] = db_connection
        end
      end

      install_logger db_connection, config

      db_connection
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