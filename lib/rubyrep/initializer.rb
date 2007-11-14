module RR

  # The Configuration class holds the default configuration options for Rubyrep.
  # Configuration values are changed with the Initializer::run method.
  class Configuration
    # Connection settings for the "left" database.
    # Takes a similar hash as ActiveRecord::Base.establish_connection.
    attr_accessor :left

    # Connection settings for the "right" database.
    # Takes a similar hash as ActiveRecord::Base.establish_connection.
    attr_accessor :right
    
    # Connection settings for the "left" Rubyrep proxy.
    # Takes a hash with the following elements
    # :host:: Mandatory: host name / IP address on which the proxy is running
    # :port:: Optional: port on which the proxy is running (default: DatabaseProxy::DEFAULT_PORT)
    attr_accessor :left_proxy
    
    # Connection settings for the "right" Rubyrep proxy.
    # See left_proxy for details.
    attr_accessor :right_proxy
    
    # General options for the proxy operation mode.
    attr_accessor :proxy_options
  end

  # The settings of the current deployment are passed to Rubyrep through the
  # Initializer::run method.
  # This method yields a Configuration object for overwriting of the default
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

    # Yields the current Configuration object to enable overwriting of
    # configuration values.
    # Refer to the Initializer class documentation for a usage example.
    def self.run
      yield configuration
    end
  end
end