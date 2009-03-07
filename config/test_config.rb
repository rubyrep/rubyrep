# Simple rubyrep configuration file for use by rspec tests.
# Can be tweaked to either use mysql or postgresql.
#
# IMPORTANT:
# Due to configuration of committer :default will NOT work for manual running
# of rubyrep commands!

database = ENV['RR_TEST_DB'] ? ENV['RR_TEST_DB'] : :postgres
# $start_proxy_as_external_process = true

load File.dirname(__FILE__) + "/#{database}_config.rb"

RR::Initializer::run do |config|
  config.options = {
    :committer => :default,
    :sync_conflict_handling => :left_wins
  }
  config.include_tables 'scanner_left_records_only'
  config.include_tables 'table_with_manual_key', :key => 'id'
end