$LOAD_PATH.unshift File.dirname(__FILE__)
$LOAD_PATH.unshift File.dirname(__FILE__) + "/rubyrep"

require 'rubygems'

gem 'activerecord', '>= 2.0.1'
require 'active_record'

require 'configuration'
require 'initializer'
require 'session'
require 'connection_extenders/registration'
require 'table_scan_helper'
require 'table_scan'
require 'type_casting_cursor'
require 'proxy_cursor'
require 'proxy_block_cursor'
require 'proxy_row_cursor'
require 'direct_table_scan'
require 'proxied_table_scan'
require 'database_proxy'
require 'proxy_runner'
require 'proxy_connection'
require 'table_spec_resolver'
require 'scan_runner'
require 'scan_summary_reporter'
require 'committers/committers'
require 'sync_helper'
require 'table_sync'

Dir["#{File.dirname(__FILE__)}/rubyrep/connection_extenders/*.rb"].each do |extender| 
  # jdbc_extender.rb is only loaded if we are running on jruby
  require extender unless extender =~ /jdbc/ and not RUBY_PLATFORM =~ /java/
end

module RR
  
end