require 'fileutils'
require 'tmpdir'
require 'tempfile'
require 'yaml'

module RR::ScanReportPrinters
  # A ScanReportPrinter producing a summary (number of differences) only.
  class ScanDetailReporter < ScanSummaryReporter
    
    # Register ScanSummaryReporter with the given command line options.
    # (Command line format as specified by OptionParser#on.)
    RR::ScanReportPrinters.register self, "-d", "--detailed",
        "Print the number of differences of each table. E. g.",
        "  left_table / right_table [differences]",
        "followed by a full dump of the differences in YAML format"
    
    # The current Session object
    attr_accessor :session

    # The temporary File receiving the differences
    attr_accessor :tmpfile

    # A scan run is to be started using this scan result printer.
    # +arg+ is the command line argument as yielded by OptionParser#on.
    def initialize(session, arg)
      super session, ""
      self.session = session
    end
    
    # A scan of the given 'left' table and corresponding 'right' table is executed.
    # Needs to yield so that the actual scan can be executed.
    def scan(left_table, right_table)

      super left_table, right_table

    ensure
      if self.tmpfile
        self.tmpfile.close
        self.tmpfile.open
        self.tmpfile.each_line {|line| puts line}
        self.tmpfile.close!
        self.tmpfile = nil
      end
    end

    # Each difference is handed to the printer as described in the format
    # as described e. g. in DirectTableScan#run
    def report_difference(type, row)
      self.tmpfile ||= Tempfile.new 'rubyrep_scan_details'
      tmpfile.puts({type => row}.to_yaml)
      super type, row
    end

    # Optional method. If a scan report printer has it, it is called after the
    # last table scan is executed.
    # (A good place to print a final summary.)
    def scanning_finished
    end
  end
end