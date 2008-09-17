require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
  end

  it "initialize should set #left and #right to empty hashes" do
    config = Configuration.new
    config.left.should == {}
    config.right.should == {}
  end
  
  it "initialize should set #options to the default options" do
    config = Configuration.new
    config.options.should == Configuration::DEFAULT_OPTIONS
  end

  it "options= should merge the provided into the existing options" do
    config = Configuration.new
    config.options = {:bla => :bla}
    config.options = {:bla => :blub}
    config.options[:bla].should == :blub
  end

  it "options_for_table should return the general options if there are no table specific options at all" do
    config = Configuration.new
    config.options_for_table('b').should == \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(config.options)
  end

  it "tables should return the list of added table specifications" do
    config = Configuration.new
    config.add_tables('a', {:bla => :blub})
    config.add_tables('a, b')
    config.add_tables(/a/)
    config.tables.should == ['a', 'a, b', /a/]
  end

  it "options_for_table should return the general options if there are no matching table specific options" do
    config = Configuration.new
    config.add_tables(/a/, {:bla => :blub})
    config.options_for_table('b').should == \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(config.options)
  end

  it "options_for_table should return table specific options mixed in with default options" do
    config = Configuration.new
    config.add_tables(/a/, {:bla => :blub})
    config.options_for_table('a').should == \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(config.options).
      merge(:bla => :blub)
  end

  it "options_for_table should return last added version of added options for matching table spec" do
    config = Configuration.new
    config.add_tables(/a/, {:bla => :blub})
    config.add_tables('a', {:bla => :blok})
    config.add_tables(/x/, {:bla => :bar})
    config.add_tables('y', {:bla => :foo})
    config.options_for_table('a').should == \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(config.options).
      merge(:bla => :blok)
  end

  it "add_options_for_table should not create table_spec duplicates" do
    config = Configuration.new
    config.add_tables(/a/, {:bla => :blub})
    config.add_tables(/a/, {:foo => :bar})
    config.options_for_table('a').should == \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(config.options).
      merge(:bla => :blub, :foo => :bar)
  end

  it "add_options_for_table should include default syncer options" do
    config = Configuration.new
    config.options = {:syncer => :one_way}

    # overwrite one syncer option
    config.add_tables(/a/, {:delete => true})

    options = config.options_for_table('a')
    Syncers::OneWaySyncer.default_options.each do |key, value|
      options[key].should == value unless key == :delete
    end
    options[:delete].should == true
  end

end