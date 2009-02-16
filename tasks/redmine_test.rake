require 'fileutils'
def install_redmine(database, name = nil)
  unless name
    install_redmine database, 'leftmine'
    install_redmine database, 'rightmine'
  else
    database = database.to_s
    database.sub!(/^postgres$/, 'postgresql')

    FileUtils.cd(File.expand_path("~"))
    unless File.exists?(name)
      system "svn checkout -r 2145 http://redmine.rubyforge.org/svn/trunk #{name}"
    end
    FileUtils.cd name
    ENV['RAILS_ENV'] = 'production'
    config = File.read('config/database.yml.example')
    config.gsub! 'redmine', name
    config.gsub! 'mysql', database
    if database == 'postgresql'
      config.gsub! 'root', 'postgres'
    end
    File.open('config/database.yml', 'w') { |f| f.write config }
    system 'rake db:drop'
    system 'rake db:create'
    system 'rake db:migrate'
    system 'echo |rake redmine:load_default_data'
  end
end

desc "Deploys two Redmine test installations"
namespace :deploy do
  task :redmine do

    database = ENV['RR_TEST_DB'] ? ENV['RR_TEST_DB'] : :postgres
    install_redmine database

    puts(<<EOS)
# Setup finished
# Start the redmine instances on ports 3000 and 3001 respectively
# with the following commands:
(cd ~/leftmine; ruby ./script/server -p 3000 -e production)
(cd ~/rightmine; ruby ./script/server -p 3001 -e production)
# Start the replication with
ruby ./bin/rubyrep replicate -c ./config/redmine_config.rb
EOS
  end
end
