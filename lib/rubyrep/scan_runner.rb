$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'drb'

module RR
  class ScanRunner
    
    # Parses the given command line parameter array
    # Returns 
    #   * the options hash or nil if command line parsing failed
    #   * status (as per UNIX conventions: 1 if parameters were invalid, 0 otherwise)
    def get_options(args)
      status = 0
      options = {}

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{__FILE__} [options]"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          options = nil
        end
      end

      begin
        parser.parse!(args)
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        options = nil
        status = 1
      end
  
      return options, status
    end
    
    # Executes a scan run based on the given options
    def scan(options)
      
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


