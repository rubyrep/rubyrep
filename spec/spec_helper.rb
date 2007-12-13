begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

require 'drb'
require 'digest/sha1'

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rubyrep'
require 'connection_extender_interface_spec'



module RR::ConnectionExtenders
  
  class << self
    alias_method :db_connect_without_cache, :db_connect unless method_defined?(:db_connect_without_cache)
    
    @@use_db_connection_cache = true
    
    # For faster spec runs, overwrite db_connect to use connection caching 
    def db_connect(config)
      config_dump = Marshal.dump config.reject {|key, | [:proxy_host, :proxy_port].include? key}
      config_checksum = Digest::SHA1.hexdigest(config_dump)
      @@db_connection_cache ||= {}
      cached_db_connection = @@db_connection_cache[config_checksum]
      if @@use_db_connection_cache and cached_db_connection and cached_db_connection.active?
        cached_db_connection
      else
        db_connection = db_connect_without_cache config
        @@db_connection_cache[config_checksum] = db_connection
        db_connection
      end
    end
    
    # If status == true: enable the cache. If status == false: don' use cache
    # Returns the old connection caching status
    def use_db_connection_cache(status)
      old_status, @@use_db_connection_cache = @@use_db_connection_cache, status
      old_status
    end
  end
end

# Creates a mock ProxySession with the given
#   * mock_table: name of the mock table
#   * primary_key_names: array of mock primary column names
#   * column_names: array of mock column names, if nil: doesn't mock this function
def create_mock_session(mock_table, primary_key_names, column_names = nil)
  session = mock("ProxySession")
  if primary_key_names
    session.should_receive(:primary_key_names) \
      .with(mock_table) \
      .and_return(primary_key_names)
  end
  if column_names
    session.should_receive(:column_names) \
      .with(mock_table) \
      .and_return(column_names)
  end
  session.should_receive(:quote_value) \
    .any_number_of_times \
    .with(an_instance_of(String), an_instance_of(String), anything) \
    .and_return {| value, column, value| value}
      
  session
end
 
# Returns a deep copy of the provided object.
def deep_copy(object)
  Marshal.restore(Marshal.dump(object))
end

# Reads the database configuration from the config folder for the specified config key
# E.g. if config is :postgres, tries to read the config from 'postgres_config.rb'
def read_config(config)
  $config_cache ||= {}
  unless $config_cache[config]
    # load the proxied config but ensure that the original configuration is restored
    old_config = RR::Initializer.configuration
    RR::Initializer.reset
    begin
      load File.dirname(__FILE__) + "/../config/#{config}_config.rb"
      $config_cache[config] = RR::Initializer.configuration
    ensure
      RR::Initializer.configuration = old_config
    end
  end
  $config_cache[config]
end

# Retrieves the proxied database config as specified in config/proxied_test_config.rb
def proxied_config
  read_config :proxied_test
end

# Retrieves the standard (non-proxied) database config as specified in config/test_config.rb
def standard_config
  read_config :test
end

# If true, start proxy as external process (more realistic test but also slower).
# Otherwise start in the current process as thread.
$start_proxy_as_external_process ||= false

# Starts a proxy under the given host and post
def start_proxy(host, port)
  if $start_proxy_as_external_process
    rrproxy_path = File.join(File.dirname(__FILE__), "..", "bin", "rrproxy.rb")
    ruby = RUBY_PLATFORM =~ /java/ ? 'jruby' : 'ruby'
    cmd = "#{ruby} #{rrproxy_path} -h #{host} -p #{port}"
    Thread.new {system cmd}    
  else
    url = "druby://#{host}:#{port}"
    DRb.start_service(url, DatabaseProxy.new)    
  end
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
    drb_url = "druby://#{proxied_config.left[:proxy_host]}:#{proxied_config.left[:proxy_port]}"
    # try to connect to the proxy
    begin
      proxy = DRbObject.new nil, drb_url
      proxy.ping
      $proxy_confirmed_running = true
    rescue DRb::DRbConnError => e
      # Proxy not yet running ==> start it
      start_proxy proxied_config.left[:proxy_host], proxied_config.left[:proxy_port]
      
      maximum_startup_time = 5 # maximum time in seconds for the proxy to start
      waiting_time = 0.1 # time to wait between connection attempts
      
      time = 0.0
      ping_response = ''
      # wait for the proxy to start up and become operational
      while ping_response != 'pong' and time < maximum_startup_time
        begin
          proxy = DRbObject.new nil, drb_url
          ping_response = proxy.ping
          break
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
        end if $start_proxy_as_external_process
      else
        raise "Could not start proxy"
      end
    end
    
    # if we got till here, then a proxy is running or was successfully started
    $proxy_confirmed_running = true
  end
end