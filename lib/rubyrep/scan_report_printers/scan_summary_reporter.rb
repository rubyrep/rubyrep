module RR::ScanReportPrinters
  # A ScanReportPrinter producing a summary (number of differences) only.
  class ScanSummaryReporter
    
    # Register ScanSummaryReporter with the given command line options.
    # (Command line format as specified by OptionParser#on.)
    RR::ScanReportPrinters.register self, "-s", "--summary[=detailed]",
        "Print the number of differences of each table. Either totals only, e. g.",
        "  left_table / right_table [differences]",
        "or a detailed split by type, e. g.",
        "  left_table / right_table [conflicts] [left_only records] [right_only records]"
    
    # Set to true if only the total number of differences should be reported
    attr_accessor :only_totals
    
    # Name of the left table of the current scan
    attr_accessor :left_table
    
    # Name of the right table of the current scan
    attr_accessor :right_table
    
    # Hold the result of the current scan. A hash with a running count of
    #  +:conflict+, +:left+ (only) or +:right+ (only) records.
    attr_accessor :scan_result

    # A scan run is to be started using this scan result printer.
    # +arg+ is the command line argument as yielded by OptionParser#on.
    def initialize(_, arg)
      self.only_totals = (arg != 'detailed')
    end
    
    # A scan of the given 'left' table and corresponding 'right' table is executed.
    # Needs to yield so that the actual scan can be executed.
    def scan(left_table, right_table)
      self.left_table = left_table
      self.right_table = right_table
      self.scan_result = {:conflict => 0, :left => 0, :right => 0}

      header = left_table.clone
      header << " / " << right_table if left_table != right_table
      $stdout.write "#{header.rjust(36)} "

      yield # Give control back so that the actual table scan can be done.

      if only_totals
        $stdout.write \
          "#{rjust_value(scan_result[:conflict] + scan_result[:left] + scan_result[:right])}"
      else
        $stdout.write \
          "#{rjust_value(scan_result[:conflict])} " +
          "#{rjust_value(scan_result[:left])} " +
          "#{rjust_value(scan_result[:right])}"
      end
      $stdout.puts
    end

    # Right adjusts the given number and returns according string.
    def rjust_value(value)
      value.to_s.rjust(3)
    end
    private :rjust_value

    # Each difference is handed to the printer as described in the format
    # as described e. g. in DirectTableScan#run
    def report_difference(type, row)
      scan_result[type] += 1
    end

    # Optional method. If a scan report printer has it, it is called after the
    # last table scan is executed.
    # (A good place to print a final summary.)
    def scanning_finished
    end
  end
end