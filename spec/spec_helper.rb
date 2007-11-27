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

# Caches the proxied database configuration
$proxied_config = nil

# Retrieves the proxied database config as specified in config/proxied_test_config.rb
def get_proxied_config
  unless $proxied_config
    # load the proxied config but ensure that the original configuration is restored
    old_config = RR::Initializer.configuration
    RR::Initializer.reset
    $proxied_config = nil
    begin
      load File.dirname(__FILE__) + '/../config/proxied_test_config.rb'
      $proxied_config = RR::Initializer.configuration
    ensure
      RR::Initializer.configuration = old_config
    end
  end
  $proxied_config
end

# Set to true if the proxy as per SPEC_PROXY_CONFIG is running 
$proxy_confirmed_running = false

# Starts a proxy as per left proxy settings defined in config/proxied_test_config.rb.
# Only starts the proxy though if none is running yet at the according host / port.
# If it starts a proxy child process, it also prepares automatic termination
# after the spec run is finished.
def ensure_proxy
  # only execute the network verification once per spec run
  unless $proxy_confirmed_running
    proxied_config = get_proxied_config
  
    drb_url = "druby://#{proxied_config.left[:proxy_host]}:#{proxied_config.left[:proxy_port]}"
    # try to connect to the proxy
    begin
      proxy = DRbObject.new nil, drb_url
      proxy.ping
      $proxy_confirmed_running = true
    rescue DRb::DRbConnError => e
      # Proxy not yet running ==> start it
      rrproxy_path = File.join(File.dirname(__FILE__), "..", "bin", "rrproxy.rb")
      cmd = "ruby #{rrproxy_path} -h #{proxied_config.left[:proxy_host]} -p #{proxied_config.left[:proxy_port]}"
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
        #puts "Proxy started (took #{time} seconds)"
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

# Get the proxied database configuration
def proxify!
  RR::Initializer.reset
  RR::Initializer.configuration = get_proxied_config
end