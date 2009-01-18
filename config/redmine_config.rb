# Test configuration file for replication between two Redmine issue tracking
# systems

database = ENV['RR_TEST_DB'] ? ENV['RR_TEST_DB'] : :postgres

load File.dirname(__FILE__) + "/#{database}_config.rb"

RR::Initializer::run do |config|
  config.left[:database] = 'leftmine'
  config.right[:database] = 'rightmine'

  config.include_tables(/./)
  config.exclude_tables 'schema_migrations'
  config.exclude_tables 'plugin_schema_infos'

  config.options[:auto_key_limit] = 2
end