require 'spec_helper'

require 'yaml'

require 'replication_extender_interface.rb'
require 'postgresql_replication.rb'

include RR

extenders = [:postgres, :mysql]

extenders.each do |extender|
  describe "#{extender.to_s.capitalize} Replication Extender" do
    before(:each) do
      @org_test_db = ENV['RR_TEST_DB']
      ENV['RR_TEST_DB'] = extender.to_s
      Initializer.configuration = standard_config
    end

    after(:each) do
      ENV['RR_TEST_DB'] = @org_test_db
    end

    include_examples "ReplicationExtender"
    include_examples "PostgreSQLReplication" if extender == :postgres
  end
end
