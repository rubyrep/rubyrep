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
    
    # Create a session on the proxy side according to provided configuration hash
    def initialize(config)
      self.connection = ConnectionExtenders.db_connect config
    end
    
    # Destroys the session
    def destroy
      self.connection.disconnect!
    end
  end
end
