begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")
require 'rubyrep'