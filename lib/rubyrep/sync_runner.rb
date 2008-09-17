$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

module RR
  # This class implements the functionality of the rrsync.rb command.
  class SyncRunner < BaseRunner
    # Returns summary description string for the scan command.
    def summary_description
      "Syncs the differences of the specified tables between both databases."
    end

    # Creates the correct scan class.
    # Parameters as defined under BaseRunner#create_processor
    def create_processor(session, left_table, right_table)
      TableSync.new session, left_table, right_table
    end
  end
end


