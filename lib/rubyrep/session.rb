module RR
  
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
      
      ActiveRecord::Base.establish_connection(configuration.left)
      self.left = ActiveRecord::Base.connection
      
      # If both database configurations point to the same database
      # then don't create the database connection twice
      if configuration.left == configuration.right
	self.right = self.left
      else
	ActiveRecord::Base.establish_connection(configuration.right)
	self.right = ActiveRecord::Base.connection
      end  
    end
  end
end
