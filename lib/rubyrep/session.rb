require 'drb'

module RR

  # This class represents a rubyrep session.
  # Creates and holds expensive objects like e. g. database connections.
  class Session
    
    # Deep copy of the original Configuration object
    attr_accessor :configuration
    
    # Returns the "left" ActiveRecord / proxy database connection
    def left
      @connections[:left]
    end
    
    # Stores the "left" ActiveRecord /proxy database connection
    def left=(connection)
      @connections[:left] = connection
    end
    
    # Returns the "right" ActiveRecord / proxy database connection
    def right
      @connections[:right]
    end
    
    # Stores the "right" ActiveRecord / proxy database connection
    def right=(connection)
      @connections[:right] = connection
    end
    
    # Hash to hold under either :left or :right the according Drb / direct DatabaseProxy
    attr_accessor :proxies
    
    # Does the actual work of establishing a database connection
    # db_arm:: should be either :left or :right
    # config:: the rubyrep Configuration
    def db_connect(db_arm, config)
      arm_config = config.send db_arm
      @proxies[db_arm] = DatabaseProxy.new
      @connections[db_arm] = @proxies[db_arm].create_session arm_config
    end
    
    # Does the actual work of establishing a proxy connection
    # db_arm:: should be either :left or :right
    # config:: the rubyrep Configuration
    def proxy_connect(db_arm, config)
      arm_config = config.send db_arm
      if arm_config.include? :proxy_host 
        drb_url = "druby://#{arm_config[:proxy_host]}:#{arm_config[:proxy_port]}"
        @proxies[db_arm] = DRbObject.new nil, drb_url
      else
        # If one connection goes through a proxy, so has the other one.
        # So if necessary, create a "fake" proxy
        @proxies[db_arm] = DatabaseProxy.new
      end
      @connections[db_arm] = @proxies[db_arm].create_session arm_config
    end
    
    # True if proxy connections are used
    def proxied?
      [configuration.left, configuration.right].any? \
        {|arm_config| arm_config.include? :proxy_host}
    end
        
    # Creates a new rubyrep session with the provided Configuration
    def initialize(config = Initializer::configuration)
      @connections = {:left => nil, :right => nil}
      @proxies = {:left => nil, :right => nil}
      
      # Keep the database configuration for future reference
      # Make a deep copy to isolate from future changes to the configuration
      self.configuration = Marshal.load(Marshal.dump(config))

      # Determine method of connection (either 'proxy_connect' or 'db_connect'
      connection_method = proxied? ? :proxy_connect : :db_connect
      
      # Connect the left database / proxy
      self.send connection_method, :left, configuration
      
      # If both database configurations point to the same database
      # then don't create the database connection twice
      if configuration.left == configuration.right
        self.right = self.left
      else
        self.send connection_method, :right, configuration
      end  
    end
  end
end
