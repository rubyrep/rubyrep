begin
  require 'rspec'
rescue LoadError
  require 'rubygems'
  require 'rspec'
end
begin
require 'rspec/core/rake_task'
rescue LoadError
  puts <<-EOS
To use rspec for testing you must install rspec gem:
    gem install rspec
  EOS
  exit(0)
end

task :spec do
  system "rspec spec"
end

namespace :spec do
  desc "Generate specdocs for examples for inclusion in RDoc"
  task :docs do
    system 'rspec --format documentation spec'
  end

  desc "Run the specs with coverage"
  task :cov do
    system 'COVERAGE=true rspec spec'
  end

  desc "Run the specs for all supported databases"
  task :all_dbs do
    [:postgres, :mysql].each do |test_db|
      puts "Running specs for #{test_db}"
      system "bash -c 'RR_TEST_DB=#{test_db} rspec spec'"
    end
  end
  
  desc "Run the specs for all supported databases and ruby platforms" 
  task :all_rubies do
    system %(rvm ruby@rubyrep,jruby@rubyrep do bash -c 'for db in postgres mysql; do echo "`rvm current` - $db:"; RR_TEST_DB=$db rspec spec; done')
  end
end
