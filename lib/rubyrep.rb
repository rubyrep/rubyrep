$LOAD_PATH.unshift File.dirname(__FILE__)
$LOAD_PATH.unshift File.dirname(__FILE__) + "/rubyrep"

require 'rubygems'
require 'active_record'

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
require 'proxy_session'

Dir["#{File.dirname(__FILE__)}/rubyrep/connection_extenders/*.rb"].each { |extender| require extender }

module RR
  
end