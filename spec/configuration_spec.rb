require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
  end

  it "initialize should set #left and #right to empty hashes" do
    config = Configuration.new
    [:left, :right].each do |hash_attr|
      config.send(hash_attr).should == {}
    end
  end
  
  it "initialize should set #proxy_options to the default proxy options" do
    config = Configuration.new
    config.proxy_options.should == Configuration::DEFAULT_PROXY_OPTIONS
  end

  it "initialize should set #sync_options to the default sync options" do
    config = Configuration.new
    config.sync_options.should == Configuration::DEFAULT_SYNC_OPTIONS
  end
  
  it "proxy_options= should set the new proxy options after merging them into the default proxy options" do
    config = Configuration.new
    config.proxy_options = {:bla => :blub}
    config.proxy_options.should == Configuration::DEFAULT_PROXY_OPTIONS.merge({:bla => :blub})
  end

  it "sync_options= should set the new syc options after merging them into the default sync options" do
    config = Configuration.new
    config.sync_options = {:bla => :blub}
    config.sync_options.should == Configuration::DEFAULT_SYNC_OPTIONS.merge({:bla => :blub})
  end
  
  it "options_for_table should return the general options if there are no table specific options at all" do
    config = Configuration.new
    config.options_for_table('b').should == {
      :proxy_options => config.proxy_options,
      :sync_options => Syncers::TwoWaySyncer.default_options.clone \
        .merge(config.sync_options)
    }
  end

  it "options_for_table should return the general options if there are no matching table specific options" do
    config = Configuration.new
    config.add_options_for_table(/a/, :sync_options => {:bla => :blub})
    config.options_for_table('b').should == {
      :proxy_options => config.proxy_options,
      :sync_options => Syncers::TwoWaySyncer.default_options.clone \
        .merge(config.sync_options)
    }
  end

  it "options_for_table should return table specific options mixed in with default options" do
    config = Configuration.new
    config.add_options_for_table(/a/, :sync_options => {:bla => :blub})
    config.options_for_table('a') \
      .should == {
      :proxy_options => config.proxy_options, 
      :sync_options => Syncers::TwoWaySyncer.default_options.clone \
        .merge(config.sync_options.merge(:bla => :blub))}
  end

  it "options_for_table should return last added version of added options for matching table spec" do
    config = Configuration.new
    config.add_options_for_table(/a/, :sync_options => {:bla => :blub})
    config.add_options_for_table('a', :sync_options => {:bla => :blok})
    config.add_options_for_table(/x/, :sync_options => {:bla => :bar})
    config.add_options_for_table('y', :sync_options => {:bla => :foo})
    config.options_for_table('a') \
      .should == {
      :proxy_options => config.proxy_options, 
      :sync_options => Syncers::TwoWaySyncer.default_options.clone \
        .merge(config.sync_options.merge(:bla => :blok))}
  end

  it "add_options_for_table should not create table_spec duplicates" do
    config = Configuration.new
    config.add_options_for_table(/a/, :sync_options => {:bla => :blub})
    config.add_options_for_table(/a/, :proxy_options => {:foo => :bar})
    config.options_for_table('a') \
      .should == {
      :proxy_options => config.proxy_options.merge(:foo => :bar), 
      :sync_options => Syncers::TwoWaySyncer.default_options.clone \
        .merge(config.sync_options.merge(:bla => :blub))}
  end

  it "add_options_for_table should include default syncer options" do
    config = Configuration.new
    config.sync_options = {:syncer => :one_way}

    # overwrite one syncer option
    config.add_options_for_table(/a/, :sync_options => {:delete => true}) 

    sync_options = config.options_for_table('a')[:sync_options]
    Syncers::OneWaySyncer.default_options.each do |key, value|
      sync_options[key].should == value unless key == :delete
    end
    sync_options[:delete].should == true
  end

end