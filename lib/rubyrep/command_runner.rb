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

    # Prints a summary help text to stderr
    def self.show_help
      $stderr.puts <<EOS
Usage: #{$0} [--help] [--version] command [parameters, ...]

Runs the specified rubyrep command.

Available commands:
EOS
      commands.sort.each do |command_name, command_hash|
        $stderr.puts "#{command_name.center(15)} #{command_hash[:description]}"
      end
    end

    # dispatch commands as per given command line parameters.
    # * +args+: array of command line parameters
    def self.run(args)
      if args.empty?
        show_help
        1
      elsif args[0] == '--help' or (args[0] == 'help' and args.size == 1)
        show_help
        0
      elsif args[0] == '--version'
        show_version
        0
      elsif commands.include? args[0]
        commands[args[0]][:command].run(args.slice(1, 1_000_000))
      else
        show_help
        1
      end
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
      CommandRunner.run([args[0], '--help'])
    end
  end
end


