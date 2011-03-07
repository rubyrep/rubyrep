$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR
  
  # Some helper functions that are of use to all TableScan classes
  module TableScanHelper
    # Compares the primary keys of left_row and right_row to determine their rank.
    # Assumes there is a function primary_key_names returning the array of primary keys
    # that are relevant for this comparison
    # 
    # Assumes that at least one of left_row and right_row is not nil
    # A nil row counts as infinite. 
    # E. g. left_row is something and right_row is nil ==> left_row is smaller ==> return -1
    def rank_rows(left_row, right_row)
      raise "At least one of left_row and right_row must not be nil!" unless left_row or right_row
      return -1 unless right_row
      return 1 unless left_row 
      rank = 0
      primary_key_names.any? do |key|
        if left_row[key].kind_of?(String)
          # When databases order strings, then 'a' < 'A' while for Ruby 'A' < 'a'
          # ==> Use a combination of case sensitive and case insensitive comparing to
          #     reproduce the database behaviour.
          rank = left_row[key].casecmp(right_row[key]) # deal with 'a' to 'B' comparisons
          rank = -(left_row[key] <=> right_row[key]) if rank == 0 # deal with 'a' to 'A' comparisons
        else
          rank = left_row[key] <=> right_row[key]
        end
        rank != 0
      end
      rank
    end

    # Returns the correct class for the table scan based on the type of the
    # session (proxied or direct).
    def self.scan_class(session)
      if session.proxied?
        ProxiedTableScan
      else
        DirectTableScan
      end
    end
  end
end
