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
    
    # Does the actual work of establishing a database connection
    # db_arm:: should be either :left or :right
    # arm_config:: hash of database connection parameters
    def db_connect(db_arm, arm_config)
      @connections[db_arm] = ConnectionExtenders.db_connect arm_config
    end
    private :db_connect
    
    # Does the actual work of establishing a proxy connection
    # db_arm:: should be either :left or :right
    # arm_config:: hash of proxy connection parameters
    def proxy_connect(db_arm, arm_config)
      if arm_config.include? :proxy_host 
        drb_url = "druby://#{arm_config[:proxy_host]}:#{arm_config[:proxy_port]}"
        @connections[db_arm] = DRbObject.new nil, drb_url
      else
        @connections[db_arm] = DatabaseProxy.new
      end
    end
    private :proxy_connect
    
    # True if proxy connections are used
    def proxied?
      [configuration.left, configuration.right].any? \
        {|arm_config| arm_config.include? :proxy_host}
    end
        
    # Creates a new rubyrep session with the provided Configuration
    def initialize(config = Initializer::configuration)
      @connections = {:left => nil, :right => nil}
      
      # Keep the database configuration for future reference
      # Make a deep copy to isolate from future changes to the configuration
      self.configuration = Marshal.load(Marshal.dump(config))

      # Determine method of connection (either 'proxy_connect' or 'db_connect'
      connection_method = proxied? ? :proxy_connect : :db_connect
      
      # Connect the left database
      self.send connection_method, :left, configuration.left
      
      # If both database configurations point to the same database
      # then don't create the database connection twice
      if configuration.left == configuration.right
        self.right = self.left
      else
        self.send connection_method, :right, configuration.right
      end  
    end
  end
end
