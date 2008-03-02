require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

extenders = [:mysql, :postgres]

extenders.each do |extender|
  describe "#{extender.to_s.capitalize} Extender" do
    before(:each) do
      Initializer.configuration = read_config(extender)
    end

    begin
      if read_config(extender).left[:adapter] != standard_config.left[:adapter]
        # If the current adapter is *not* the adapter for the standard tests
        # (meaning the adapter which is used to run all other tests)
        # then only run the extender spec if the database connection is available
        Session.new read_config(extender)
      end
      it_should_behave_like "ConnectionExtender"
    rescue Exception => e
      at_exit do
        puts "#{__FILE__}:#{__LINE__}: DB Connection failed with '#{e}' ==> #{extender.to_s.capitalize} not tested"
      end
    end
  end
end
