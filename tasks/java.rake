namespace :deploy do

  desc "Create the java installation package"
  task :java do
    pkg_name = "rubyrep-#{RR::VERSION::STRING}"

    system "rm -rf /tmp/#{pkg_name}"
    system "mkdir /tmp/#{pkg_name}"
    system "git archive master |tar -x -C /tmp/#{pkg_name}"
    system "mkdir -p /tmp/#{pkg_name}/jruby"
    system "cp -r #{JRUBY_HOME}/* /tmp/#{pkg_name}/jruby/"
    system "cd /tmp/#{pkg_name}/jruby; rm -rf samples share/ri lib/ruby/gems/1.8/doc"
    system "chmod a+x /tmp/#{pkg_name}/rubyrep"
    system "cd /tmp; rm -f #{pkg_name}.zip; zip -r #{pkg_name}.zip #{pkg_name} >/dev/null"
    system "mkdir -p pkg"
    system "cp /tmp/#{pkg_name}.zip pkg"
  end
end