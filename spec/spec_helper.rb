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

unless self.class.const_defined?('STRANGE_TABLE')
  if ENV['RR_TEST_DB'] == 'postgres' || ENV['RR_TEST_DB'] == nil
    STRANGE_TABLE = 'table_with.stränge Name山'
  else
    STRANGE_TABLE = 'table_with_stränge Name山'
  end
  STRANGE_COLUMN = 'stränge. Column山'
end

class Module
  # Used to verify that an instance of the class / module receives a call of the
  # specified method.
  # This is for cases where a method call has to be mocked of an object that is 
  # not yet created. 
  # (Couldn't find out how to do that using existing rspec mocking features.)
  def any_instance_should_receive(method, &blck)
    tmp_method = "original_before_mocking_#{method}".to_sym
    logger_key = "#{self.name}_#{method}"
    $mock_method_marker ||= {}
    $mock_method_marker[logger_key] = Spec::Mocks::Mock.new("#{name} Instance")
    $mock_method_marker[logger_key].should_receive(method).at_least(:once)
    self.send :alias_method, tmp_method, method
    self.class_eval "def #{method}(*args); $mock_method_marker['#{logger_key}'].#{method}; end"
    blck.call
  ensure
    $mock_method_marker.delete logger_key
    self.send :alias_method, method, tmp_method rescue nil
  end

  # Used to verify that an instance of the class / module does not receive a 
  # call of the specified method.
  # This is for cases where a method call has to be mocked of an object that is 
  # not yet created. 
  # (Couldn't find out how to do that using existing rspec mocking features.)
  def any_instance_should_not_receive(method, &blck)
    tmp_method = "original_before_mocking_#{method}".to_sym
    logger_key = "#{self.name}_#{method}"
    $mock_method_marker ||= {}
    $mock_method_marker[logger_key] = Spec::Mocks::Mock.new("#{name} Instance")
    $mock_method_marker[logger_key].should_not_receive(method)
    self.send :alias_method, tmp_method, method
    self.class_eval "def #{method}(*args); $mock_method_marker['#{logger_key}'].#{method}; end"
    blck.call
  ensure
    $mock_method_marker.delete logger_key
    self.send :alias_method, method, tmp_method rescue nil
  end
end

class RR::Session
  # To keep rspec output of failed tests managable
  def inspect; 'session'; end
end

class ActiveRecord::Base
  class << self
    # Hack:
    # The default inspect method (as per activerecord version 2.2.2) tries to
    # send commands to the database.
    # This leads to rcov failing.
    # As workaround this is disabling the attempts to connect to the database.
    def inspect
      super
    end
  end
end

# If number_of_calls is :once, mock ActiveRecord for 1 call.
# If number_of_calls is :twice, mock ActiveRecord for 2 calls.
def mock_active_record(number_of_calls)
  ConnectionExtenders::DummyActiveRecord.should_receive(:establish_connection).send(number_of_calls) \
    .and_return {|config| $used_config = config}
    
  dummy_connection = Object.new
  # We have a spec testing behaviour for non-existing extenders.
  # So extend might not be called in all cases
  dummy_connection.stub!(:extend)
  dummy_connection.stub!(:tables).and_return([])
  dummy_connection.stub!(:initialize_search_path)
  dummy_connection.stub!(:select_one).and_return({'x' => '2'})
    
  ConnectionExtenders::DummyActiveRecord.should_receive(:connection).send(number_of_calls) \
    .and_return {dummy_connection}
end

# Creates a mock ProxyConnection with the given
#   * mock_table: name of the mock table
#   * primary_key_names: array of mock primary column names
#   * column_names: array of mock column names, if nil: doesn't mock this function
def create_mock_proxy_connection(mock_table, primary_key_names, column_names = nil)
  session = mock("ProxyConnection")
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
    .and_return { |table, column, value| value}
  
  session.should_receive(:connection) \
    .any_number_of_times \
    .and_return {dummy_connection}
  
  session.should_receive(:quote_column_name) \
    .any_number_of_times \
    .with(an_instance_of(String)) \
    .and_return { |column_name| "'#{column_name}'" }
      
  session.should_receive(:quote_table_name) \
    .any_number_of_times \
    .with(an_instance_of(String)) \
    .and_return { |table_name| "'#{table_name}'" }
      
  session
end

# Turns an SQL query into a regular expression:
#   * Handles quotes (differing depending on DBMS).
#   * Handles round brackets (escaping with backslash to make them literals).
#   * Removes line breaks and double spaces 
#     (allowing use of intendation and line continuation)
# Returns the regular expression created from the provided +sql+ string.
def sql_to_regexp(sql)
  Regexp.new(sql.strip.squeeze(" ") \
      .gsub("(", "\\(").gsub(")", "\\)") \
      .gsub("'", 'E?.') \
      .gsub('"', 'E?.'))
end
  
# Returns a deep copy of the provided object. Works also for Proc objects or
# objects referencing Proc objects.
def deep_copy(object)
  Proc.send :define_method, :_dump, lambda { |depth|
    @@proc_store ||= {}
    @@proc_key ||= "000000000"
    @@proc_key.succ!
    @@proc_store[@@proc_key] = self
    @@proc_key
  }
  Proc.class.send :define_method, :_load, lambda { |key|
    proc = @@proc_store[key]
    @@proc_store.delete key
    proc
  }

  Marshal.restore(Marshal.dump(object))
ensure
  Proc.send :remove_method, :_dump if Proc.method_defined? :_dump
  Proc.class.send :remove_method, :_load if Proc.class.method_defined? :_load
end

# Allows the temporary faking of RUBY_PLATFORM to the given value
# Needs to be called with a block. While the block is executed, RUBY_PLATFORM
# is set to the given fake value
def fake_ruby_platform(fake_ruby_platform)
  old_ruby_platform = RUBY_PLATFORM
  old_verbose, $VERBOSE = $VERBOSE, nil
  Object.const_set 'RUBY_PLATFORM', fake_ruby_platform
  $VERBOSE = old_verbose
  yield
ensure
  $VERBOSE = nil
  Object.const_set 'RUBY_PLATFORM', old_ruby_platform
  $VERBOSE = old_verbose
end

# Reads the database configuration from the config folder for the specified config key
# E.g. if config is :postgres, tries to read the config from 'postgres_config.rb'
def read_config(config)
  $config_cache ||= {}
  cache_key = "#{config.to_s}_#{ENV['RR_TEST_DB']}"
  unless $config_cache[cache_key]
    # load the proxied config but ensure that the original configuration is restored
    old_config = RR::Initializer.configuration
    RR::Initializer.reset
    begin
      load File.dirname(__FILE__) + "/../config/#{config}_config.rb"
      $config_cache[cache_key] = RR::Initializer.configuration
    ensure
      RR::Initializer.configuration = old_config
    end
  end
  $config_cache[cache_key]
end

# Removes all cached database configurations
def clear_config_cache
  $config_cache = {}
end

# Retrieves the proxied database config as specified in config/proxied_test_config.rb
def proxied_config
  read_config :proxied_test
end

# Retrieves the standard (non-proxied) database config as specified in config/test_config.rb
def standard_config
  read_config :test
end

# Inserts two records into 'sequence_test' and returns the generated id values
# * session: the active Session
# * table: name of the table which is to be tested
def get_example_sequence_values(session, table = 'sequence_test')
  session.left.insert_record table, { 'name' => 'bla' }
  id1 = session.left.select_one("select max(id) as id from #{table}")['id'].to_i
  session.left.insert_record table, { 'name' => 'blub' }
  id2 = session.left.select_one("select max(id) as id from #{table}")['id'].to_i
  return id1, id2
end

# If true, start proxy as external process (more realistic test but also slower).
# Otherwise start in the current process as thread.
$start_proxy_as_external_process ||= false

# Starts a proxy under the given host and post
def start_proxy(host, port)
  if $start_proxy_as_external_process
    bin_path = File.join(File.dirname(__FILE__), "..", "bin", "rubyrep")
    ruby = RUBY_PLATFORM =~ /java/ ? 'jruby' : 'ruby'
    cmd = "#{ruby} #{bin_path} proxy -h #{host} -p #{port}"
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