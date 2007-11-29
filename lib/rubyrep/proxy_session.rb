$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'

module RR

  # This class represents a remote rubyrep session.
  # DatabaseProxy creates one for each client connection.
  # Creates and holds expensive objects like e. g. database connections.
  class ProxySession
    
    # The database connection
    attr_accessor :connection
    
    # hash of proxy options
    attr_accessor :proxy_options
    
    # Create a session on the proxy side according to provided configuration hash and proxy_options hash
    def initialize(config, proxy_options)
      self.connection = ConnectionExtenders.db_connect config
      self.proxy_options = proxy_options
    end
    
    # Destroys the session
    def destroy
      self.connection.disconnect!
    end
    
    # Returns an array of primary key names for the given table
    def primary_key_names(table)
      self.connection.primary_key_names table
    end
  end
end
