$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'thread'
require 'monitor'

class Monitor
  alias lock mon_enter
  alias unlock mon_exit
end

module RR
  # This class implements the functionality of the 'replicate' command.
  class ReplicationRunner

    CommandRunner.register 'replicate' => {
      :command => self,
      :description => 'Starts a replication process'
    }
    
    # Provided options. Possible values:
    # * +:config_file+: path to config file
    attr_accessor :options

    # Should be set to +true+ if the replication runner should be terminated.
    attr_accessor :termination_requested

    # Parses the given command line parameter array.
    # Returns the status (as per UNIX conventions: 1 if parameters were invalid,
    # 0 otherwise)
    def process_options(args)
      status = 0
      self.options = {}

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} replicate [options]

  Replicates two databases as per specified configuration file.
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
        unless @config
          load options[:config_file]
          @config = Initializer.configuration
        end
        @session = Session.new @config
      end
      @session
    end

    # Removes current +Session+.
    def clear_session
      @session = nil
    end

    # Wait for the next replication time
    def pause_replication
      @last_run ||= 1.year.ago
      now = Time.now
      @next_run = @last_run + session.configuration.options[:replication_interval]
      unless now >= @next_run
        waiting_time = @next_run - now
        @waiter_thread.join waiting_time
      end
      @last_run = Time.now
    end

    # Initializes the waiter thread used for replication pauses and processing
    # the process TERM signal.
    def init_waiter
      @termination_mutex = Monitor.new
      @termination_mutex.lock
      @waiter_thread ||= Thread.new {@termination_mutex.lock; self.termination_requested = true}
      %w(TERM INT).each do |signal|
        Signal.trap(signal) {puts "\nCaught '#{signal}': Initiating graceful shutdown"; @termination_mutex.unlock}
      end
    end

    # Prepares the replication
    def prepare_replication
      initializer = ReplicationInitializer.new session
      initializer.prepare_replication
    end

    # Executes a single replication run
    def execute_once
      session.refresh
      timeout = session.configuration.options[:database_connection_timeout]
      terminated = TaskSweeper.timeout(timeout) do |sweeper|
        run = ReplicationRun.new session, sweeper
        run.run
      end.terminated?
      raise "replication run timed out" if terminated
    rescue Exception => e
      clear_session
      raise e
    end

    # Executes an endless loop of replication runs
    def execute
      init_waiter
      prepare_replication

      until termination_requested do
        begin
          execute_once
        rescue Exception => e
          now = Time.now.iso8601
          $stderr.puts "#{now} Exception caught: #{e}"
          if @last_exception_message != e.to_s # only print backtrace if something changed
            @last_exception_message = e.to_s
            $stderr.puts e.backtrace.map {|line| line.gsub(/^/, "#{' ' * now.length} ")}
          end
        end
        pause_replication
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


