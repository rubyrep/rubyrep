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

    # Returns the active ScanReportPrinter (as selected through the ScanRunner
    # command line options OR if none was selected, the default one).
    def active_printer
      @active_printer ||= ScanReportPrinters::ScanSummaryReporter.new(nil)
    end

    # Sets the active ScanReportPrinter
    attr_writer :active_printer

    # Parses the given command line parameter array.
    # Returns
    #   * the options hash or nil if command line parsing failed.
    #     Hash values:
    #       * +:config_file+: path to config file
    #       * +:table_specs+: array of table specification strings
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

        ScanReportPrinters.on_printer_selection(opts) do |printer|
          self.active_printer = printer
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
    # session is the current Session.
    def create_processor(session, left_table, right_table)
      # not implemented in the base class
    end

    # Executes a run based on the given options.
    # +options+ is a hash as returned by #get_options.
    def execute(options)
      load options[:config_file]
      session = Session.new Initializer.configuration
      resolver = TableSpecResolver.new session
      table_specs = resolver.resolve options[:table_specs]
      table_specs.each do |table_spec|
        active_printer.scan table_spec[:left_table], table_spec[:right_table] do
          processor = create_processor session,
            table_spec[:left_table], table_spec[:right_table]
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

      options, status = runner.get_options(args)
      if options
        runner.execute options
      end
      status
    end

  end
end


