require 'fileutils'
require 'tmpdir'
require 'tempfile'
require 'yaml'

module RR::ScanReportPrinters
  # A ScanReportPrinter producing a summary (number of differences) only.
  class ScanDetailReporter < ScanSummaryReporter
    
    # Register ScanSummaryReporter with the given command line options.
    # (Command line format as specified by OptionParser#on.)
    RR::ScanReportPrinters.register self, "-d", "--detailed[=mode]",
      "Print the number of differences of each table. E. g.",
      "  left_table / right_table [differences]",
      "followed by a full dump of the differences in YAML format.",
      "The 'mode' argument determines how the row differences are printed:",
      " * full shows the full records",
      " * keys shows the primary key columns only",
      " * diff shows the primary key and differing columsn only"
    
    # The current Session object
    attr_accessor :session

    # The temporary File receiving the differences
    attr_accessor :tmpfile

    # Mode of reporting. Should be either
    # * :+full+
    # * :+keys+ or
    # * :+diff+
    attr_accessor :report_mode

    # Array of names of the primary key columns of the table currently being
    # scanned.
    attr_accessor :primary_key_names

    # A scan run is to be started using this scan result printer.
    # +arg+ is the command line argument as yielded by OptionParser#on.
    def initialize(session, arg)
      super session, ""
      self.session = session

      self.report_mode = case arg
      when 'diff'
        :diff
      when 'keys'
        :keys
      else
        :full
      end
    end
    
    # A scan of the given 'left' table and corresponding 'right' table is executed.
    # Needs to yield so that the actual scan can be executed.
    def scan(left_table, right_table)

      super left_table, right_table

    ensure
      self.primary_key_names = nil
      if self.tmpfile
        self.tmpfile.close
        self.tmpfile.open
        self.tmpfile.each_line {|line| puts line}
        self.tmpfile.close!
        self.tmpfile = nil
      end
    end

    # Returns a cleaned row as per current +report_mode+.
    # +row+ is either a column_name => value hash or an array of 2 such rows.
    def clear_columns(row)
      case report_mode
      when :full
        row
      when :keys
        row = row[0] if row.kind_of?(Array)
        self.primary_key_names ||= session.left.primary_key_names(self.left_table)
        row.reject {|column, value| !self.primary_key_names.include?(column)}
      when :diff
        self.primary_key_names ||= session.left.primary_key_names(self.left_table)
        if row.kind_of?(Array)
          new_row_array = [{}, {}]
          row[0].each do |column, value|
            if self.primary_key_names.include?(column) or value != row[1][column]
              new_row_array[0][column] = row[0][column]
              new_row_array[1][column] = row[1][column]
            end
          end
          new_row_array
        else
          row
        end
      end
    end

    # Each difference is handed to the printer as described in the format
    # as described e. g. in DirectTableScan#run
    def report_difference(type, row)
      self.tmpfile ||= Tempfile.new 'rubyrep_scan_details'
      tmpfile.puts({type => clear_columns(row)}.to_yaml)
      super type, row
    end

    # Optional method. If a scan report printer has it, it is called after the
    # last table scan is executed.
    # (A good place to print a final summary.)
    def scanning_finished
    end
  end
end