require 'config/requirements'
require 'config/hoe' if Object.const_defined? 'Hoe' # setup Hoe + all gem configuration

require 'lib/rubyrep'
require 'tasks/task_helper'

Dir['tasks/**/*.rake'].each { |rake| load rake }
load 'sims/big_scan/big_scan.rake'

desc "Creates the SVN statistics"
task :statsvn do
  jar_path = '~/usr/statsvn-0.3.1/statsvn.jar'
  log_path = File.dirname(__FILE__) + '/tmp/statsvn.log'
  checkout_path = File.dirname(__FILE__)
  svnstats_dir = File.dirname(__FILE__) + '/statsvn'

  system 'svn update'
  cmd = "svn log -v --xml >#{log_path}"
  system cmd
  cmd = "java -jar #{jar_path} -output-dir #{svnstats_dir} -exclude 'setup.rb:website/**' #{log_path} #{checkout_path}"
  system cmd
end