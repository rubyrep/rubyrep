require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

require 'replication_extender_interface_spec.rb'
require 'postgresql_replication_spec.rb'

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

    begin
      if ENV['RR_TEST_DB'] != @org_test_db.to_s
        # If the current adapter is *not* the adapter for the standard tests
        # (meaning the adapter which is used to run all other tests)
        # then only run the extender spec if the database connection is available
        Session.new read_config(extender)
      end
      it_should_behave_like "ReplicationExtender"
      it_should_behave_like "PostgreSQLReplication" if extender == :postgres
    rescue Exception => e
      at_exit do
        puts "#{__FILE__}:#{__LINE__}: DB Connection failed with '#{e}' ==> #{extender.to_s.capitalize} replication extender not tested"
      end
    end
  end
end
