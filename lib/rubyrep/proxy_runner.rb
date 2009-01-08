$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'drb'

module RR
  # This class implements the functionality of the rrproxy.rb command.
  class ProxyRunner

    CommandRunner.register 'proxy' => {
      :command => self,
      :description => 'Proxies connections from rubyrep commands to the database'
    }
    
    # Default options to start a DatabaseProxy server
    DEFAULT_OPTIONS = {
      :port => DatabaseProxy::DEFAULT_PORT,
      :host => ''
    }
    
    # Parses the given command line parameter array.
    # Returns 
    #   * the options hash or nil if command line parsing failed
    #   * status (as per UNIX conventions: 1 if parameters were invalid, 0 otherwise)
    def get_options(args)
      options = DEFAULT_OPTIONS
      status = 0

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{$0} proxy [options]"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on("-h","--host", "=IP_ADDRESS", "IP address to listen on. Default: binds to all IP addresses of the computer") do |arg|
          options[:host] = arg
        end

        opts.on("-p","--port", "=PORT_NUMBER", Integer, "TCP port to listen on. Default port: #{DatabaseProxy::DEFAULT_PORT}") do |arg|
          options[:port] = arg
        end

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

    # Builds the druby URL from the given options and returns it
    def build_url(options)
      "druby://#{options[:host]}:#{options[:port]}"
    end

    # Starts a proxy server under the given druby URL
    def start_server(url)
      proxy = DatabaseProxy.new

      DRb.start_service(url, proxy)
      DRb.thread.join
    end
    
    # Runs the ProxyRunner (processing of command line & starting of server)
    # args: the array of command line options with which to start the server
    def self.run(args)
      runner = ProxyRunner.new
      
      options, status = runner.get_options(args)
      if options
        url = runner.build_url(options)
        runner.start_server(url)
      end
      status
    end

  end
end


