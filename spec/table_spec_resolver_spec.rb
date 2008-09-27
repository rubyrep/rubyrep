require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSpecResolver do
  before(:each) do
    Initializer.configuration = standard_config
    @session = Session.new
    @resolver = TableSpecResolver.new @session
  end

  it "initialize should store the session and cache the tables of the session" do
    @resolver.session.should == @session
    @session.should_not_receive :right # ensure that actually the 'left' tables are taken
    @resolver.tables.should == @session.left.tables
  end
  
  it "resolve should resolve direct table names correctly" do
    @resolver.resolve(['bla', 'blub']).should == [
      {:left => 'bla', :right => 'bla'},
      {:left => 'blub', :right => 'blub'}
    ]
  end
  
  it "resolve should resolve table name pairs correctly" do
    @resolver.resolve(['my_left_table , my_right_table']).should == [
      {:left => 'my_left_table', :right => 'my_right_table'}
    ]
  end
  
  it "resolve should resolve string in form of regular expression correctly" do
    @resolver.resolve(['/SCANNER_RECORDS|scanner_text_key/']).sort { |a,b|
      a[:left] <=> b[:left]
    }.should == [
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'scanner_text_key', :right => 'scanner_text_key'}
    ]
  end

  it "resolve should resolve regular expressions correctly" do
    @resolver.resolve([/SCANNER_RECORDS|scanner_text_key/]).sort { |a,b|
      a[:left] <=> b[:left]
    }.should == [
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'scanner_text_key', :right => 'scanner_text_key'}
    ]
  end

  it "resolve should should not return the same table multiple times" do
    @resolver.resolve([
        'scanner_records',
        'scanner_records',
        'scanner_records, bla',
        '/scanner_records/'
      ]
    ).should == [
      {:left => 'scanner_records', :right => 'scanner_records'}
    ]
  end
end