namespace :deploy do

  BASH_FILE_CONTENT = <<'EOS'
#!/bin/bash

script_dir="`dirname \"$0\"`"

jruby_path="$script_dir"/jruby/bin/jruby
rubyrep_path="$script_dir"/bin/rubyrep

$jruby_path --server $rubyrep_path $*
EOS

  BAT_FILE_CONTENT = <<'EOS'.gsub(/^(.*)$/,"\\1\r")
@echo off
set jruby_path=%~dp0jruby\bin\jruby.bat
set rubyrep_path=%~dp0bin\rubyrep
%jruby_path% --server %rubyrep_path% %1 %2 %3 %4 %5 %6 %7 %8 %9
EOS

  desc "Create the java installation package"
  task :java do
    pkg_name = "rubyrep-#{RR::VERSION::STRING}"

    system "rm -rf /tmp/#{pkg_name}"
    system "mkdir /tmp/#{pkg_name}"
    system "git archive master |tar -x -C /tmp/#{pkg_name}"
    system "mkdir -p /tmp/#{pkg_name}/jruby"
    system "cp -r #{JRUBY_HOME}/* /tmp/#{pkg_name}/jruby/"
    system "cd /tmp/#{pkg_name}/jruby; rm -rf samples share/ri lib/ruby/gems/1.8/doc"
    File.open("/tmp/#{pkg_name}/rubyrep.bat", 'w') {|f| f.write(BAT_FILE_CONTENT)}
    File.open("/tmp/#{pkg_name}/rubyrep", 'w') {|f| f.write(BASH_FILE_CONTENT)}
    system "chmod a+x /tmp/#{pkg_name}/rubyrep"
    system "cd /tmp; rm -f #{pkg_name}.zip; zip -r #{pkg_name}.zip #{pkg_name} >/dev/null"
    system "mkdir -p pkg"
    system "cp /tmp/#{pkg_name}.zip pkg"
  end
end