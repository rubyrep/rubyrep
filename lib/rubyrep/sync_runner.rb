$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

module RR
  # This class implements the functionality of the rrsync.rb command.
  class SyncRunner < BaseRunner

    # If set to true, disable the automatic ordering that is done to avoid running
    # into foreign key constraint violations.
    attr_accessor :no_table_ordering

    # Returns summary description string for the scan command.
    def summary_description
      "Syncs the differences of the specified tables between both databases."
    end

    # Add SyncRunner specific options to the provided OptionParser object.
    def add_specific_options(opts)
      opts.on("--no-table-ordering", "Disable automatic table ordering") do
        self.no_table_ordering = true
      end
    end

    # Creates the correct scan class.
    # Parameters as defined under BaseRunner#create_processor
    def create_processor(left_table, right_table)
      TableSync.new session, left_table, right_table
    end
  end
end


