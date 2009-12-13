require 'fileutils'
include FileUtils

require 'rubygems'

# Load essential gems. Exit if not available.
%w[rake active_record].each do |req_gem|
  begin
    require req_gem
  rescue LoadError
    puts "This Rakefile requires the '#{req_gem}' RubyGem."
    puts "Installation: gem install #{req_gem} -y"
    exit
  end  
end

# Load gem builder / ruby-forge integration gems. Complain but continue if not available.
%w[hoe].each do |req_gem|
  begin
    require req_gem
  rescue LoadError
    # The jruby platform is probably only used for testing. 
    # So if on jruby, do not even complain.
    if not RUBY_PLATFORM =~ /java/
      puts "Without #{req_gem} rake tasks to build the gem / integrate the RubyForge will not be available."
    end
  end
end

$:.unshift(File.join(File.dirname(__FILE__), %w[.. lib]))

require 'rubyrep'