require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

extenders = {
  :mysql => ConnectionExtenders::MysqlExtender,
  :postgres => ConnectionExtenders::PostgreSQLExtender
}

extenders.each do |extender_key, extender_class|
  describe extender_class do
    before(:each) do
      Initializer.configuration = read_config(extender_key)
    end

    begin
      Session.new read_config(extender_key)
      it_should_behave_like "ConnectionExtender"
    rescue Exception => e
      at_exit do
        puts "#{__FILE__}:#{__LINE__}: DB Connection failed with '#{e}' ==> #{extender_class} not tested"
      end
    end
  end
end
