$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

module RR
  # This class implements the functionality of the rrscan.rb command.
  class ScanRunner < BaseRunner

    CommandRunner.register 'scan' => {
      :command => self,
      :description => 'Scans for differing records between databases'
    }

    # Returns summary description string for the scan command.
    def summary_description
      "Scans for differences of the specified tables between both databases."
    end
    
    # Creates the correct scan class.
    # Parameters as defined under BaseRunner#create_processor
    def create_processor(left_table, right_table)
      TableScanHelper.scan_class(session).new session, left_table, right_table
    end
  end
end


