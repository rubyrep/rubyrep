$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'

module RR
  # This class implements the functionality of the 'uninstall' command.
  class UninstallRunner

    CommandRunner.register 'uninstall' => {
      :command => self,
      :description => 'Removes all rubyrep tables, triggers, etc. from "left" and "right" database'
    }
    
    # Provided options. Possible values:
    # * +:config_file+: path to config file
    attr_accessor :options

    # Parses the given command line parameter array.
    # Returns the status (as per UNIX conventions: 1 if parameters were invalid,
    # 0 otherwise)
    def process_options(args)
      status = 0
      self.options = {}

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} uninstall [options]

  Removes all rubyrep tables, triggers, etc. from "left" and "right" database.
EOS
        opts.separator ""
        opts.separator "  Specific options:"

        opts.on("-c", "--config", "=CONFIG_FILE",
          "Mandatory. Path to configuration file.") do |arg|
          options[:config_file] = arg
        end

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          self.options = nil
        end
      end

      begin
        parser.parse!(args)
        if options # this will be +nil+ if the --help option is specified
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

    # Returns the active +Session+.
    # Loads config file and creates session if necessary.
    def session
      unless @session
        load options[:config_file]
        @session = Session.new Initializer.configuration
      end
      @session
    end

    # Removes all rubyrep created database objects.
    def execute
      initializer = ReplicationInitializer.new session
      initializer.restore_unconfigured_tables([])
      initializer.drop_infrastructure
      puts "Uninstall completed: rubyrep tables and triggers removed!"
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


