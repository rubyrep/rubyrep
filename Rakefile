require 'config/requirements'
require 'config/hoe' if Object.const_defined? 'Hoe' # setup Hoe + all gem configuration

require 'lib/rubyrep'
require 'tasks/task_helper'

Dir['tasks/**/*.rake'].each { |rake| load rake }
load 'sims/performance/performance.rake'

desc "Creates the repository commit statistics"
task :repostats do
  # phase 0: create the repository tmp directory
  system 'mkdir -p tmp'
  # phase 1: migrate the hg repository to svn
  tailor_path = '~/usr/tailor/tailor'
  cmd = "#{tailor_path} --use-propset --configfile '#{File.dirname(__FILE__) + '/tasks/rubyrep.tailor'}'"
  system cmd
  
  # phase 2: create the repository statistics through the statsvn library
  jar_path = '~/usr/statsvn/statsvn.jar'
  log_path = File.dirname(__FILE__) + '/tmp/statsvn.log'
  checkout_path = '/tmp/rubyrep_tailor/svn'
  svnstats_dir = File.dirname(__FILE__) + '/statsvn'

  system "cd #{checkout_path}; svn update"
  cmd = "cd #{checkout_path}; svn log -v --xml >#{log_path}"
  system cmd
  cmd = "java -jar #{jar_path} -output-dir #{svnstats_dir} -exclude 'setup.rb:website/**' #{log_path} #{checkout_path}"
  system cmd
end