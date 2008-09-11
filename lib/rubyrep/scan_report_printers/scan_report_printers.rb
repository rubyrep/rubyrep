module RR
  # Manages scan report printers. Scan report printers implement functionality
  # to report the row differences identified during a scan.
  #
  # Scan report printers need to register themselves and their command line options
  # with #register.
  #
  # A scan report printer neesd to implement at the minimum the following
  # functionality:
  #
  #   # Creation of a new ScanReportPrinter.
  #   # +arg+ is the command line argument as yielded by OptionParser#on.
  #   def initialize(arg)
  #
  #   # A scan of the given 'left' table and corresponding 'right' table is executed.
  #   # Needs to yield so that the actual scan can be executed.
  #   def scan(left_table, right_table)
  #
  #   # Each difference is handed to the printer as described in the format
  #   # as described e. g. in DirectTableScan#run
  #   def report_difference(type, row)
  #
  #   # Optional method. If a scan report printer has it, it is called after the
  #   # last table scan is executed.
  #   # (A good place to print a final summary.)
  #   def scanning_finished
  #
  module ScanReportPrinters

    # Array of registered ScanReportPrinters.
    # Each entry is a hash with the following keys:
    # * +:printer_class+: The ScanReportPrinter class.
    # * +:opts+: An array defining the command line options (handed to OptionParter#on).
    def self.printers
      @@report_printers ||= []
    end

    # Needs to be called by ScanReportPrinters to register themselves (+printer+)
    # and their command line options.
    # +:printer_class+ is the ScanReportPrinter class,
    # +:opts+ is an array defining the command line options (handed to OptionParter#on).
    def self.register(printer_class, *opts)
      printers << {
        :printer_class => printer_class,
        :opts => opts
      }
    end

    # Registers all report printers command line options into the given
    # OptionParser.
    # Once the command line is parsed with OptionParser#parse! it will
    # create the correct printer as per specified command line options and
    # yield it.
    #
    # Note: if multiple printers are specified in the command line,
    # all are created and yielded.
    def self.on_printer_selection(opts)
      printers.each do |printer|
        opts.on(*printer[:opts]) do |arg|
          yield printer[:printer_class].new(arg)
        end
      end
    end
  end
end