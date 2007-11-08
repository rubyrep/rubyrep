require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe ConnectionExtenders::PostgreSQLExtender do
  before(:each) do
    Initializer.reset
    load File.dirname(__FILE__) + '/../config/test_config.rb'
  end

  it "primary_key_names should return primary key names ordered as per primary key index" do
    session = Session.new
    session.left.primary_key_names('posts_tags').should == ['post_id', 'tag_id']
    
    session.left.primary_key_names('posts_tags_with_inverted_primary_key_index') \
      .should == ['tag_id', 'post_id']
  end
  
  it "primary_key_names should return an empty array for tables without any primary key" do
    session = Session.new
    session.left.primary_key_names('posts_tags_without_primary_key') \
      .should == []
  end
  
  it "primary_key_names called for a non-existing table should throw an exception" do
    session = Session.new
    lambda {session.left.primary_key_names('non_existing_table')} \
      .should raise_error(RuntimeError, 'table does not exist')
  end
end