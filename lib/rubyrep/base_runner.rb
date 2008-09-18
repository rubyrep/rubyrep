$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'

module RR
  # This class implements the base functionality for runners that process table
  # specs.
  class BaseRunner

    # Default options if not overriden in command line
    DEFAULT_OPTIONS = {
      :table_specs => []
    }

    # Provided options. Possible values:
    # * +:config_file+: path to config file
    # * +:table_specs+: array of table specification strings
    attr_accessor :options

    # Returns the active ScanReportPrinter (as selected through the ScanRunner
    # command line options OR if none was selected, the default one).
    def active_printer
      @active_printer ||= ScanReportPrinters::ScanSummaryReporter.new(nil)
    end

    # Sets the active ScanReportPrinter
    attr_writer :active_printer

    # Returns the default command summary description (nothing).
    # Should be overwritten by child classes.
    def summary_description; ""; end

    # Parses the given command line parameter array.
    # Returns the status (as per UNIX conventions: 1 if parameters were invalid,
    # 0 otherwise)
    def process_options(args)
      status = 0
      self.options = DEFAULT_OPTIONS

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} [options] [table_spec] [table_spec] ...

  #{summary_description}

  table_spec can be either:
    * a specific table name (e. g. 'users') or
    * a pair of (specific) table names (e. g.: 'users,users_backup')
        (In this case the first table in the 'left' database is compared
         with the second table in the 'right' database.)
    * a regular expression (e. g. '/^user/') [case insensitive match]
  If no table_specs are provided via command line, the ones from the
  configuration file are used.
EOS
        opts.separator ""
        opts.separator "  Specific options:"

        ScanReportPrinters.on_printer_selection(opts) do |printer|
          self.active_printer = printer
        end

        opts.on("-c", "--config", "=CONFIG_FILE",
          "Mandatory. Path to configuration file.") do |arg|
          options[:config_file] = arg
        end
        
        add_specific_options(opts)

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          self.options = nil
        end
      end

      begin
        unprocessed_args = parser.parse!(args)
        if options # this will be +nil+ if the --help option is specified
          options[:table_specs] = unprocessed_args
          raise("Please specify configuration file") unless options.include?(:config_file)
        end
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        self.options = nil
        status = 1
      end

      return status
    end

    # Signals scan completion to the (active) scan report printer if it supports
    # that method.
    def signal_scanning_completion
      if active_printer.respond_to? :scanning_finished
        active_printer.scanning_finished
      end
    end

    # Creates a processor that does something with the given table.
    # A processor needs to implement a +run+ method that yields for progress
    # reporting purposes pairs of diff_type and row as defined under
    # DirectTableScan#run.
    def create_processor(left_table, right_table)
      # not implemented in the base class
    end

    # Intended to be overwritten by derived classes to need to add additional
    # options to the provided +OptionParser+ object.
    def add_specific_options(opts)
    end

    # Intended to be overwritten by derived classes that need to modify the
    # table_pairs.
    # * session: the active +Session+
    # * table_pairs: array of table pairs as returned by TableSpecResolver#resolve
    # Returns the new table pairs array.
    def prepare_table_pairs(session, table_pairs)
      table_pairs
    end
    
    # Returns the active +Session+. 
    # Loads config file if necessary and creates session if necessary.
    def session
      unless @session
        load options[:config_file]
        @session = Session.new Initializer.configuration
      end
      @session
    end

    # Executes a run based on the established options.
    def execute
      resolver = TableSpecResolver.new session

      # Use the command line provided table specs if provided. Otherwise the
      # ones from the configuration file
      table_specs = options[:table_specs]
      table_specs = Initializer.configuration.tables if table_specs.empty?
      
      table_pairs = resolver.resolve table_specs
      table_pairs = prepare_table_pairs(session, table_pairs)
      table_pairs.each do |table_pair|
        active_printer.scan table_pair[:left_table], table_pair[:right_table] do
          processor = create_processor \
            table_pair[:left_table], table_pair[:right_table]
          processor.run do |diff_type, row|
            active_printer.report_difference diff_type, row
          end
        end
      end
      signal_scanning_completion
    end

    # Entry points for executing a processing run.
    # args: the array of command line options that were provided by the user.
    def self.run(args)
      runner = new

      status = runner.process_options(args)
      if runner.options
        runner.execute
      end
      status
    end

  end
end


