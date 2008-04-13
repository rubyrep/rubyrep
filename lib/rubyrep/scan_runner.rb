$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'drb'

module RR
  # This class implements the functionality of the rrscan.rb command.
  #
  # Output of scan results is done by separate scan report printers.
  # Those printers need to register themselves and their command line options
  # with #register_printer.
  # The printers need to implement at the minimum the following functionality:
  #
  #   # ScanRunner creates the ScanReportPrinter that is to be used.
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
  class ScanRunner
    
    # Default options if not overriden in command line
    DEFAULT_OPTIONS = {
      :table_specs => []
    }
    
    # Returns the active ScanReportPrinter (as selected through the ScanRunner 
    # command line options OR if none was selected, the default one).
    def active_printer
      @active_printer ||= ScanSummaryReporter.new(nil)
    end
    
    # Sets the active ScanReportPrinter
    attr_writer :active_printer
    
    # Array of registered ScanReportPrinters.
    # Each entry is a hash with the following keys:
    # * +:printer_class+: The ScanReportPrinter class.
    # * +:opts+: An array defining the command line options (handed to OptionParter#on).
    def self.report_printers
      @@report_printers ||= []
    end
    
    # Parses the given command line parameter array.
    # Returns 
    #   * the options hash or nil if command line parsing failed.
    #     Hash values:
    #       * +:config_file+: path to config file
    #       * +:table_specs: array of table specification strings
    #   * status (as per UNIX conventions: 1 if parameters were invalid, 0 otherwise)
    def get_options(args)
      status = 0
      options = DEFAULT_OPTIONS

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} [options] table_spec [table_spec] ...        
  table_spec can be either: 
    * a specific table name (e. g. 'users') or
    * a pair of (specific) table names (e. g.: 'users,users_backup')
        (In this case the first table in the 'left' database is compared
         with the second table in the 'right' database.)
    * a regular expression (e. g. '/^user/') [case insensitive match]
EOS
        opts.separator ""
        opts.separator "Specific options:"

        self.class.report_printers.each do |printer|
          opts.on(*printer[:opts]) do |arg|
            self.active_printer = printer[:printer_class].new(arg)
          end
        end

        opts.on("-c", "--config", "=CONFIG_FILE", 
          "Mandatory. Path to configuration file.") do |arg|
          options[:config_file] = arg
        end
        
        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          options = nil
        end
      end

      begin
        unprocessed_args = parser.parse!(args)
        if options # this will be +nil+ if the --help option is specified
          options[:table_specs] = unprocessed_args
          raise("Please specify configuration file") unless options.include?(:config_file)
          raise("Please provide at least one table_spec") if options[:table_specs].empty?
        end
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        options = nil
        status = 1
      end
  
      return options, status
    end
    
    # Executes a scan run based on the given options.
    # +options+ is a hash as returned by #get_options.
    def scan(options)
      load options[:config_file]
      session = Session.new Initializer.configuration
    end

    # Needs to be called by ScanReportPrinters to register themselves (+printer+)
    # and their command line options.
    # +:printer_class+ is the ScanReportPrinter class, 
    # +:opts+ is an array defining the command line options (handed to OptionParter#on).
    def self.register_printer(printer_class, *opts)
      report_printers << {
        :printer_class => printer_class,
        :opts => opts
      }
    end
    
    # Runs the ProxyRunner (processing of command line & starting of server)
    # args: the array of command line options with which to start the server
    def self.run(args)
      runner = ScanRunner.new
      
      options, status = runner.get_options(args)
      if options
        runner.scan options
      end
      status
    end

  end
end


