$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'

module RR

  # This class implements the functionality to dispatch rubyrep commands.
  class CommandRunner
    
    # Returns a hash of all commands registered with #register.
    def self.commands
      @commands ||= {}
    end

    # Registers one or multiple commands.
    # +commands+ is a hash with
    # * key: name of the command
    # * value: a command hash defining the command
    #
    # A command hash consists of
    # * :+description+: short description of the command
    # * :+command+: an object / class implementing the hash.
    #               Must have a method
    #
    #               # runs a command
    #               # * +args+: array of command line parameters
    #               #           note: will not contain the command name itself.
    #               def run(args)
    def self.register(commands)
      self.commands.merge!(commands)
    end
    
    # Prints the version to stderr
    def self.show_version
      $stdout.puts "rubyrep version #{RR::VERSION::STRING}"
    end


    # Dispatches commands as per given command line parameters.
    # * +args+: array of command line parameters
    def self.run(args)
      status = 0
      options = {}

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} [general options] command [parameters, ...]

Asynchronous master-master replication of relational databases.
EOS
        opts.separator ""
        opts.separator "Available options:"

        opts.on("--verbose", "Show errors with full stack trace") do
          options[:verbose] = true
        end

        opts.on("-v", "--version", "Show version information.") do
          show_version
          options = nil
        end

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          
          $stderr.puts "\nAvailable commands:"
          commands.sort.each do |command_name, command_hash|
            $stderr.puts "  #{command_name.ljust(15)} #{command_hash[:description]}"
          end

          options = nil
        end
      end

      begin

        # extract general options
        general_args = []
        until args.empty?
          if args[0] =~ /^-/
            general_args << args.shift
          else
            break
          end
        end

        # parse general options
        parser.parse!(general_args)

        # process commands
        if options # this will be +nil+ if the --help or --version are specified
          if args.empty?
            $stderr.puts "No command specified.\n\n"
            run(['--help'])
            status = 1
          else
            command = args[0]
            if command == 'help' and args.size == 1
              run(['--help'])
              status = 0
            elsif commands.include? command
              status = commands[command][:command].run(args.slice(1, 1_000_000))
            else
              $stderr.puts "Error: Unknown command specified.\n\n"
              run(['--help'])
              status = 1
            end
          end
        end
      rescue Exception => e
        $stderr.puts "Exception caught: #{e}"
        $stderr.puts e.backtrace if options && options[:verbose]
        status = 1
      end

      return status
    end
  end

  # Command runner to show help for other commands
  class HelpRunner
    CommandRunner.register 'help' => {
      :command => self,
      :description => "Shows detailed help for the specified command"
    }

    # Runs the help command
    # * +args+: array of command line parameters
    def self.run(args)
      if args[0] == 'help' or args[0] == '--help'
        $stderr.puts(<<EOS)
Usage: #{$0} help [command]

Shows the help for the specified command.
EOS
        0
      else
        CommandRunner.run([args[0], '--help'])
      end
    end
  end
end


