database = ENV['RR_TEST_DB'] ? ENV['RR_TEST_DB'] : :postgres
# $start_proxy_as_external_process = true

load File.dirname(__FILE__) + "/#{database}_config.rb"

RR::Initializer::run do |config|
  config.options = {
    :committer => :default,
    :syncer => :two_way,
    :conflict_handling => :update_right
  }
  config.add_tables 'scanner_left_records_only'
  config.add_tables 'table_with_manual_key', :primary_key_names => ['id']
end