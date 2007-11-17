begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")
require 'rubyrep'

module RR
  class Session
    
    # Disable the Session caching during the next session creation
    def self.clear_config_cache
      @@old_config = nil
    end
    
    # Speed up spec runs by only creating new Sessions if the configuration changed.
    def self.new(config = Initializer::configuration)
      @@old_config ||= nil
      if Marshal.dump(@@old_config) != Marshal.dump(config)
        @@old_config = config
        @@old_session = super config
      else
        @@old_session
      end
    end

  end
end