begin
  require 'code_statistics' # This library is coming with the rails gem
  desc "Report code statistics (KLOCs, etc)"
  task :stats do
    STATS_DIRECTORIES = [
      %w(Libraries          lib/),
      %w(Unit\ tests        spec/),
      %w(Integration\ tests sims/)
    ].collect { |name, dir| [ name, "#{File.dirname(__FILE__)}/../#{dir}" ] }.select { |name, dir| File.directory?(dir) }

    desc "Report code statistics (KLOCs, etc) from the application"
    task :stats do
      require 'code_statistics'
      CodeStatistics.new(*STATS_DIRECTORIES).to_s
    end
    
  end
rescue LoadError
end
