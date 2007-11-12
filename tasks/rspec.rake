begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  require 'spec'
end
begin
  require 'spec/rake/spectask'
rescue LoadError
  puts <<-EOS
To use rspec for testing you must install rspec gem:
    gem install rspec
  EOS
  exit(0)
end

desc "Run the specs under spec/models"
Spec::Rake::SpecTask.new do |t|
  t.spec_opts = ['--options', "spec/spec.opts"]
  t.spec_files = FileList['spec/*_spec.rb']
end

namespace :spec do
  desc "Generate specdocs for examples for inclusion in RDoc"
  Spec::Rake::SpecTask.new('docs') do |t|
    t.spec_files = FileList['spec/*_spec.rb']
    t.spec_opts = ["--format", "specdoc"]
  end

  desc "Run the specs with RCov"
  Spec::Rake::SpecTask.new('rcov') do |t|
    t.spec_opts = ['--options', "spec/spec.opts"]
    t.spec_files = FileList['spec/*_spec.rb']
    t.rcov = true
    t.rcov_opts = ['--exclude', 'tasks/,spec/,gems/\(?!rubyrep\)']
  end
  
  begin
    require 'ruby-prof/task'
    RubyProf::ProfileTask.new do |t|
      t.test_files = FileList['spec/*_spec.rb']
      t.output_dir = 'profile'
      t.printer = :flat
      t.min_percent = 1
    end
  rescue LoadError
  end
end