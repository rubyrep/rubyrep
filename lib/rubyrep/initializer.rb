module RR
  
  # The Configuration class holds the default configuration options for Rubyrep.
  # Configuration values are changed with the Initializer::run method.
  class Configuration
    # connection settings for the "left" database
    attr_accessor :left
    
    # connection settings for the "right" database
    attr_accessor :right

  end
  
  # The settings of the current deployment are passed to Rubyrep through the
  # Initializer::run method.
  # The method yields the a Configuration object for overwriting of the default
  # settings.
  # Accordingly a configuration file should look something like this:
  #
  #   Rubyrep::Initializer.run do |config|
  #     config.left = ...
  #   end
  class Initializer

    # Sets a new Configuration object
    # Current configuration values are lost and replaced with the default 
    # settings.
    def self.reset
      @@configuration = Configuration.new
    end
    reset
    
    # Returns the current Configuration object
    def self.configuration
      @@configuration
    end
    
    # Yields the current Configuration object to enabl overwriting of
    # configuration values.
    # See class documentation for usage example.
    def self.run
      yield configuration
    end
  end
end