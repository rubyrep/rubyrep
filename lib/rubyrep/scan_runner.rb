$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'drb'

module RR
  class ScanRunner
    
    # Default options if not overriden in command line
    DEFAULT_OPTIONS = {
      :table_specs => []
    }
    
    # Parses the given command line parameter array
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
        opts.banner = "Usage: #{__FILE__} [options] table_spec [table_spec] ...
          table_spec can be either: 
            * a specific table name (e. g. 'users') or
            * a pair of (specific) table names (e. g.: 'users,users_backup')
                (In this case the first table in the 'left' database is compared
                 with the second table in the 'right' database.)
            * a regular expression (e. g. '/^user/')"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on("-c","--config", "=CONFIG_FILE", 
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
    # Options is as returned by #get_options.
    def scan(options)
      load options[:config_file]
      session = Session.new Initializer.configuration
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


