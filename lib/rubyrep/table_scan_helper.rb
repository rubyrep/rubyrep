$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'rubyrep'

module RR
  
  # Some helper functions that are of use to all TableScan classes
  module TableScanHelper
    # Compare the primary keys of left_row and right_row to determine their rank
    # Assumes there is a function primary_key_names returning the array of primary keys
    # that are relevant for this comparison
    def rank_rows(left_row, right_row)
      rank = 0
      primary_key_names.any? do |key|
        rank = left_row[key] <=> right_row[key]
        rank != 0
      end
      rank
    end
  end
end
