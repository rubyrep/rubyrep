module RR

  # Dummy ActiveRecord descendants to keep the connection objects
  # (Without it the ActiveRecord datase connection doesn't work)
  class Left < ActiveRecord::Base
  end

  # Dummy ActiveRecord descendants to keep the connection objects
  # (Without it the ActiveRecord datase connection doesn't work)
  class Right < ActiveRecord::Base
  end

  # This class represents a rubyrep session
  # Creating and holding expensive objective like e. g. database connections
  class Session
    
    # Deep copy of the original Configuration object
    attr_accessor :configuration
    
    @@active_record_holders = {:left => Left, :right => Right}
    
    # The "left" ActiveRecord database connection
    def left
      @connections[:left]
    end
    
    def left=(connection)
      @connection[:left] = connection
    end
    
    # The "right" ActiveRecord database connection
    def right
      @connections[:right]
    end
    
    def right=(connection)
      @connections[:right] = connection
    end
    
    # Does the actual work of establishing a database connection
    # db_arm:: should be either :left or :right
    # config:: hash of connection parameters
    def db_connect(db_arm, config)
      @@active_record_holders[db_arm].establish_connection(config)
      @connections[							db_arm] = @@active_record_holders[db_arm].connection
    end
    private :db_connect
    
    # Creates a new rubyrep session based on the provided Configuration
    def initialize(config = Initializer::configuration)
      @connections = {:left => nil, :right => nil}
      
      # Keep the database configuration for future reference
      # Make a deep copy to isolate from future changes to the configuration
      self.configuration = Marshal.load(Marshal.dump(config))

      db_connect :left, configuration.left
      
      # If both database configurations point to the same database
      # then don't create the database connection twice
      if configuration.left == configuration.right
	self.right = self.left
      else
	db_connect :right, configuration.right
      end  
    end
  end
end
