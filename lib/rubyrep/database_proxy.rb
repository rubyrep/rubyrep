$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR
  # The proxy to a remote database connection
  class DatabaseProxy
    
    # Default tcp port to listen on
    DEFAULT_PORT = 9876
    
    # A simple Hash to hold Session object
    # Purpose: preventing them from being garbage collected when they are only referenced through Drb
    attr_accessor :session_register
    
    def initialize
      self.session_register = {}
    end
    
    # Create a ProxySession according to provided configuration Hash
    def create_session(config)
      session = ProxySession.new config
      self.session_register[session] = session
      session
    end
    
    # Destroys the given session from the session register
    def destroy_session(session)
      session.destroy
      session_register.delete session
    end
  end
end

