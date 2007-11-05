$LOAD_PATH.unshift File.dirname(__FILE__)
$LOAD_PATH.unshift File.dirname(__FILE__) + "/rubyrep"

require 'rubygems'
require 'active_record'

require 'initializer'
require 'session'
require 'connection_extenders/registration'

Dir["#{File.dirname(__FILE__)}/rubyrep/connection_extenders/*.rb"].each { |extender| require extender }

module RR
  
end