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
    
    # The "left" and "right" ActiveRecord database connections
    attr_accessor :left, :right   
    
    # Creates a new rubyrep session based on the provided Configuration
    def initialize(config = Initializer::configuration)
      
      # Keep the database configuration for future reference
      # Make a deep copy to isolate from future changes to the configuration
      self.configuration = Marshal.load(Marshal.dump(config))

      Left.establish_connection(configuration.left)
      self.left = Left.connection
      
      # If both database configurations point to the same database
      # then don't create the database connection twice
      if configuration.left == configuration.right
	self.right = self.left
      else
	Right.establish_connection(configuration.right)
	self.right = Right.connection
      end  
    end
  end
end
