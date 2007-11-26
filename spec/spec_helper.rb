begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

require 'drb'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")
require 'rubyrep'

SPEC_PROXY_CONFIG = {
  :proxy_host => '127.0.0.1',
  :proxy_port => '9876'
}

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

# Set to true if the proxy as per SPEC_PROXY_CONFIG is running 
$proxy_confirmed_running = false

# Stars a proxy as per SPEC_PROXY_CONFIG (but only if not yet running).
# If it starts a proxy child process, it also prepares automatic termination
# after the spec run is finished.
def ensure_proxy
  # only execute the network verification once per spec run
  unless $proxy_confirmed_running
    drb_url = "druby://#{SPEC_PROXY_CONFIG[:proxy_host]}:#{SPEC_PROXY_CONFIG[:proxy_port]}"
    # try to connect to the proxy
    begin
      proxy = DRbObject.new nil, drb_url
      proxy.ping
      $proxy_confirmed_running = true
    rescue DRb::DRbConnError => e
      # Proxy not yet running ==> start it
      rrproxy_path = File.join(File.dirname(__FILE__), "..", "bin", "rrproxy.rb")
      cmd = "ruby #{rrproxy_path} -h #{SPEC_PROXY_CONFIG[:proxy_host]} -p #{SPEC_PROXY_CONFIG[:proxy_port]}"
      Thread.new {system cmd}
      
      maximum_startup_time = 5 # maximum time in seconds for the proxy to start
      waiting_time = 0.1 # time to wait between connection attempts
      
      time = 0.0
      ping_response = ''
      # wait for the proxy to start up and become operational
      while ping_response != 'pong' and time < maximum_startup_time
        begin
          proxy = DRbObject.new nil, drb_url
          ping_response = proxy.ping
        rescue DRb::DRbConnError => e
          # do nothing (just try again)
        end
        sleep waiting_time
        time += waiting_time
      end
      if ping_response == 'pong'
        puts "Proxy started (took #{time} seconds)"
        # Ensure that the started proxy is terminated with the completion of the spec run.
        at_exit do
          proxy = DRbObject.new nil, drb_url
          proxy.terminate! rescue DRb::DRbConnError
        end
      else
        raise "Could not start proxy"
      end
    end
    
    # if we got till here, then a proxy is running or was successfully started
    $proxy_confirmed_running = true
  end
end

# Adds proxy options to the left database configuration
def proxify!
  Initializer::run do |config|
    config.left.merge!(SPEC_PROXY_CONFIG)
  end
end