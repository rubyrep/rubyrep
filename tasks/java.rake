namespace :deploy do
  desc "Create the java installation package"
  task :java do
    pkg_name = "rubyrep-#{RR::VERSION::STRING}"

    system "rm -rf /tmp/#{pkg_name}"
    system "hg archive /tmp/#{pkg_name}"
    system "mkdir -p /tmp/#{pkg_name}/jruby"
    system "cp -r #{JRUBY_HOME}/* /tmp/#{pkg_name}/jruby/"
    system "cd /tmp/#{pkg_name}/jruby; rm -rf samples share/ri lib/ruby/gems/1.8/doc"
    File.open("/tmp/#{pkg_name}/rubyrep", 'w') do |f|
      f.write(<<EOS)
#!/bin/bash

script_dir="`dirname \"$0\"`"

jruby_path="$script_dir"/jruby/bin/jruby
rubyrep_path="$script_dir"/bin/rubyrep

$jruby_path $rubyrep_path $*
EOS
    end
    system "chmod a+x /tmp/#{pkg_name}/rubyrep"
    system "cd /tmp; rm -f #{pkg_name}.tar.gz; tar -czf #{pkg_name}.tar.gz #{pkg_name}"
    system "mkdir -p pkg"
    system "cp /tmp/#{pkg_name}.tar.gz pkg"
  end
end