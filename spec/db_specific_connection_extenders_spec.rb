require 'spec_helper'

require 'yaml'

require 'connection_extender_interface.rb'

include RR

extenders = [:mysql, :postgres]

extenders.each do |extender|
  describe "#{extender.to_s.capitalize} Connection Extender" do
    before(:each) do
      @org_test_db = ENV['RR_TEST_DB']
      ENV['RR_TEST_DB'] = extender.to_s
      Initializer.configuration = standard_config
    end

    after(:each) do
      ENV['RR_TEST_DB'] = @org_test_db
    end

    include_examples "ConnectionExtender"

  end
end
