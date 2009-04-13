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
  end

  it "tables should return the tables of the specified database" do
    @resolver.tables(:left).should == @session.left.tables
    @resolver.tables(:right).should == @session.right.tables
  end
  
  it "resolve should resolve direct table names correctly" do
    @resolver.resolve(['scanner_records', 'referenced_table']).should == [
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'referenced_table', :right => 'referenced_table'}
    ]
  end
  
  it "resolve should resolve table name pairs correctly" do
    @resolver.resolve(['left_table , right_table']).should == [
      {:left => 'left_table', :right => 'right_table'}
    ]
  end
  
  it "resolve should complain about non-existing tables" do
    lambda {@resolver.resolve(['dummy, scanner_records'])}.
      should raise_error(/non-existing.*dummy/)
    lambda {@resolver.resolve(['left_table, left_table'])}.
      should raise_error(/non-existing.*left_table/)
    lambda {@resolver.resolve(['left_table'])}.
      should raise_error(/non-existing.*left_table/)
  end

  it "resolve should not complain about regexp specified tables not existing in right database" do
    @resolver.resolve([/^scanner_records$/, /left_table/]).
      should == [{:left => 'scanner_records', :right => 'scanner_records'}]
  end

  it "resolve should not check for non-existing tables if that is disabled" do
    lambda {@resolver.resolve(['dummy, scanner_records'], [], false)}.
      should_not raise_error
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

  it "resolve should not return tables that are excluded" do
    @resolver.resolve(
      [/SCANNER_RECORDS|scanner_text_key/],
      [/scanner_text/]
    ).should == [
      {:left => 'scanner_records', :right => 'scanner_records'},
    ]
  end

  it "non_existing_tables should return an empty hash if all tables exist" do
    table_pairs = [{:left => 'scanner_records', :right => 'referenced_table'}]
    @resolver.non_existing_tables(table_pairs).should == {}
  end

  it "non_existing_tables should return a hash of non-existing tables" do
    table_pairs = [{:left => 'scanner_records', :right => 'bla'}]
    @resolver.non_existing_tables(table_pairs).should == {:right => ['bla']}

    table_pairs = [
      {:left => 'blub', :right => 'bla'},
      {:left => 'scanner_records', :right => 'xyz'}
      ]
    @resolver.non_existing_tables(table_pairs).should == {
      :left => ['blub'],
      :right => ['bla', 'xyz']
    }
  end

end