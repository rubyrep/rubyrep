# Simple rubyrep configuration file.
# Intendet to be used for manual tests of the rubyrep commands.
#
# IMPORTANT:
# After completion of manual tests, use the 'db:test:rebuild' command to
# recreate the test databases.
# Otherwise the rspec tests will fail.

database = ENV['RR_TEST_DB'] ? ENV['RR_TEST_DB'] : :postgres
# $start_proxy_as_external_process = true

load File.dirname(__FILE__) + "/#{database}_config.rb"

RR::Initializer::run do |config|
  config.options = {
  }
  config.include_tables 'scanner_left_records_only'
  config.include_tables 'table_with_manual_key', :key => 'id'
  config.include_tables 'extender_combined_key'
end