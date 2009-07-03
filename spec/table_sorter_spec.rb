require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSorter do
  before(:each) do
  end

  it "sort should order the tables correctly" do
    tables = [
      'scanner_records',
      'referencing_table',
      'referenced_table',
      'scanner_text_key',
    ]

    sorter = TableSorter.new Session.new(standard_config), tables
    sorted_tables = sorter.sort

    # make sure it contains the original tables
    sorted_tables.sort.should == tables.sort

    # make sure the referenced table comes before the referencing table
    sorted_tables.grep(/referenc/).should == ['referenced_table', 'referencing_table']

    # verify that we are using TSort#tsort to get that result
    sorter.should_not_receive(:tsort)
    sorter.sort
  end
end
