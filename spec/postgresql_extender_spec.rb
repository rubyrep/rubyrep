require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe ConnectionExtenders::PostgreSQLExtender do
  before(:each) do
  end

  it "primary_key_names should return primary key names ordered as per primary key index" do
    load File.dirname(__FILE__) + '/../config/test_config.rb'
    session = Session.new
    session.left.primary_key_names('posts_tags').should == ['post_id', 'tag_id']
    
    session.left.primary_key_names('posts_tags_with_inverted_primary_key_index') \
      .should == ['tag_id', 'post_id']
  end
end