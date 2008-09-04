require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Syncers::TwoWaySyncer do
  before(:each) do
    Initializer.configuration = deep_copy(standard_config)
    Initializer.configuration.sync_options = {:syncer => :two_way}
  end

  it "should register itself" do
    Syncers::syncers[:two_way].should == Syncers::TwoWaySyncer
  end

  it "initialize should store sync_helper" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    syncer = Syncers::TwoWaySyncer.new(helper)
    syncer.sync_helper.should == helper
  end

  it "initialize should throw an error if options are invalid" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    base_options = {
      :syncer => :two_way,
      :left_record_handling => :ignore,
      :right_record_handling => :ignore,
      :conflict_handling => :ignore
    }

    # Verify that correct options don't raise errors.
    helper.stub!(:sync_options).and_return(base_options)
    lambda {Syncers::TwoWaySyncer.new(helper)}.should_not raise_error

    # Also lambda options should not raise errors.
    l = lambda {}
    helper.stub!(:sync_options).and_return(base_options.merge(
        {
          :left_record_handling => l,
          :right_record_handling => l,
          :conflict_handling => l
        })
    )
    lambda {Syncers::TwoWaySyncer.new(helper)}.should_not raise_error

    # Invalid options should raise errors
    invalid_options = [
      {:left_record_handling => :invalid_left_option},
      {:right_record_handling => :invalid_right_option},
      {:conflict_handling => :invalid_conflict_option}
    ]
    invalid_options.each do |options|
      helper.stub!(:sync_options).and_return(base_options.merge(options))
      lambda {Syncers::TwoWaySyncer.new(helper)}.should raise_error(ArgumentError)
    end
  end

  it "sync_difference should not do anything if ignore option is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => :ignore,
        :right_record_handling => :ignore,
        :conflict_handling => :ignore
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    helper.should_not_receive(:delete_record)
    helper.should_not_receive(:update_record)
    helper.should_not_receive(:insert_record)

    [:left, :right, :conflict].each do |diff_type|
      syncer.sync_difference(diff_type, :dummy_row)
    end
  end

  it "sync_difference should call the provided Proc objects" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)

    lambda_parameters = []
    l = lambda do |sync_helper, type, row|
      lambda_parameters << [sync_helper, type, row]
    end
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => l,
        :right_record_handling => l,
        :conflict_handling => l
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    syncer.sync_difference(:left, :dummy_left)
    syncer.sync_difference(:right, :dummy_right)
    syncer.sync_difference(:conflict, [:dummy_left2, :dummy_right2])

    lambda_parameters.should == [
      [helper, :left, :dummy_left],
      [helper, :right, :dummy_right],
      [helper, :conflict, [:dummy_left2, :dummy_right2]]
    ]
  end

  it "sync_difference should delete left or right records from source if that option is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => :delete,
        :right_record_handling => :delete,
        :conflict_handling => :ignore
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    helper.should_receive(:delete_record).with(:left, :dummy_left)
    helper.should_receive(:delete_record).with(:right, :dummy_right)
    syncer.sync_difference(:left, :dummy_left)
    syncer.sync_difference(:right, :dummy_right)
  end

  it "sync_difference should insert left or right records to target if that option is given" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => :insert,
        :right_record_handling => :insert,
        :conflict_handling => :ignore
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    helper.should_receive(:insert_record).with(:right, :dummy_left)
    helper.should_receive(:insert_record).with(:left, :dummy_right)
    syncer.sync_difference(:left, :dummy_left)
    syncer.sync_difference(:right, :dummy_right)
  end

  it "sync_difference should update the left database if conflict handling is specified with :update_left" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => :ignore,
        :right_record_handling => :ignore,
        :conflict_handling => :update_left
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    helper.should_receive(:update_record).with(:left, :dummy_right)
    syncer.sync_difference(:conflict, [:dummy_left, :dummy_right])
  end

  it "sync_difference should update the right database if conflict handling is specified with :update_right" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    helper.stub!(:sync_options).and_return(
      {
        :left_record_handling => :ignore,
        :right_record_handling => :ignore,
        :conflict_handling => :update_right
      })

    syncer = Syncers::TwoWaySyncer.new(helper)
    helper.should_receive(:update_record).with(:right, :dummy_left)
    syncer.sync_difference(:conflict, [:dummy_left, :dummy_right])
  end
end