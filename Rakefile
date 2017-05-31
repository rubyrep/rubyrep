namespace :bundler do
  require "bundler/gem_tasks"
end

require_relative 'lib/rubyrep'
require_relative 'tasks/task_helper'

Dir['tasks/**/*.rake'].each { |rake| load rake }
load 'sims/performance/performance.rake'