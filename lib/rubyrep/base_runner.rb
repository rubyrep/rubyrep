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

    # The class for the selected report printer
    attr_accessor :report_printer_class

    # The specified option parameter for the report printer
    attr_accessor :report_printer_arg

    # Returns the active ScanReportPrinters instance (as selected through the
    # command line options OR if none was selected, the default one).
    def report_printer
      unless @report_printer
        printer_class = report_printer_class || ScanReportPrinters::ScanSummaryReporter
        @report_printer ||= printer_class.new(session, report_printer_arg)
      end
      @report_printer
    end

    # Returns the command line selected ScanProgressPrinters class
    attr_accessor :selected_progress_printer

    # Returns the active ScanProgressPrinter class (as selected through the
    # command line options OR if none was selected, the default one).
    def progress_printer
      if selected_progress_printer
        selected_progress_printer
      else
        printer_key = session.configuration.options[:scan_progress_printer]
        ScanProgressPrinters.printers[printer_key][:printer_class]
      end
    end

    # Returns the default command summary description (nothing).
    # Should be overwritten by child classes.
    def summary_description; ""; end

    # Parses the given command line parameter array.
    # Returns the status (as per UNIX conventions: 1 if parameters were invalid,
    # 0 otherwise)
    def process_options(args)
      status = 0
      self.options = DEFAULT_OPTIONS.clone

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} #{self.class.name.sub(/^.*::(.*)Runner$/, '\\1').downcase} [options] [table_spec] [table_spec] ...

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

        ScanReportPrinters.on_printer_selection(opts) do |printer_class, arg|
          self.report_printer_class = printer_class
          self.report_printer_arg = arg
        end

        ScanProgressPrinters.on_printer_selection(opts) do |printer|
          self.selected_progress_printer = printer
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
      if report_printer.respond_to? :scanning_finished
        report_printer.scanning_finished
      end
    end

    # Creates a processor that does something with the given table.
    # A processor needs to implement a +run+ method that yields for progress
    # reporting purposes pairs of diff_type and row as defined under
    # DirectTableScan#run.
    def create_processor(left_table, right_table)
      # not implemented in the base class
    end

    # Intended to be overwritten by derived classes that need to add additional
    # options to the provided +OptionParser+ object.
    def add_specific_options(opts)
    end

    # Intended to be overwritten by derived classes that need to modify the
    # table_pairs.
    # * table_pairs: array of table pairs as returned by TableSpecResolver#resolve
    # Returns the new table pairs array.
    def prepare_table_pairs(table_pairs)
      table_pairs
    end
    
    # Returns the active +Session+. 
    # Loads config file and creates session if necessary.
    def session
      unless @session
        load options[:config_file]
        @session = Session.new Initializer.configuration
      end
      @session
    end

    attr_writer :session

    # Returns the table pairs that should be processed.
    # Refer to TableSpecRsolver#resolve for format of return value.
    def table_pairs
      prepare_table_pairs(session.configured_table_pairs(options[:table_specs]))
    end

    # Executes a run based on the established options.
    def execute
      session.configuration.exclude_rubyrep_tables
      table_pairs.each do |table_pair|
        report_printer.scan table_pair[:left], table_pair[:right] do
          processor = create_processor \
            table_pair[:left], table_pair[:right]
          processor.progress_printer = progress_printer
          processor.run do |diff_type, row|
            report_printer.report_difference diff_type, row
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


