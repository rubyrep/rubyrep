$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'

module RR
  # This class implements the functionality of the 'generate' command.
  class GenerateRunner

    CONFIG_TEMPLATE = <<EOF
RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'postgresql', # or 'mysql'
    :database => 'SCOTT',
    :username => 'scott',
    :password => 'tiger',
    :host     => '172.16.1.1'
  }

  config.right = {
    :adapter  => 'postgresql',
    :database => 'SCOTT',
    :username => 'scott',
    :password => 'tiger',
    :host     => '172.16.1.2'
  }

  config.include_tables 'dept'
  config.include_tables /^e/ # regexp matching all tables starting with e
  # config.include_tables /./ # regexp matching all tables in the database
end
EOF

    CommandRunner.register 'generate' => {
      :command => self,
      :description => 'Generates a configuration file template'
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
Usage: #{$0} generate [file_name]

  Generates a configuration file template under name [file_name].
EOS
        opts.separator ""
        opts.separator "  Specific options:"

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          self.options = nil
        end
      end

      begin
        unprocessed_args = parser.parse!(args)
        if options # this will be +nil+ if the --help option is specified
          raise("Please specify the name of the configuration file") if unprocessed_args.empty?
          options[:file_name] = unprocessed_args[0]
        end
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        self.options = nil
        status = 1
      end

      return status
    end

    # Generates a configuration file template.
    def execute
      if File.exists?(options[:file_name])
        raise("Cowardly refuse to overwrite existing file '#{options[:file_name]}'")
      end
      File.open(options[:file_name], 'w') do |f|
        f.write CONFIG_TEMPLATE
      end
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


