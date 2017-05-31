namespace :deploy do

  desc "Create the java installation package"
  task :java do
    jruby_version='9.1.10.0'
    pkg_name = "rubyrep-#{RR::VERSION}"

    system "rm -rf /tmp/#{pkg_name}"
    system "mkdir /tmp/#{pkg_name}"
    system "git archive master |tar -x -C /tmp/#{pkg_name}"
    system "curl -o /tmp/#{pkg_name}/jruby.tar.gz https://s3.amazonaws.com/jruby.org/downloads/#{jruby_version}/jruby-bin-#{jruby_version}.tar.gz"
    system "tar -C /tmp/#{pkg_name} -xzf /tmp/#{pkg_name}/jruby.tar.gz"
    system "mv /tmp/#{pkg_name}/jruby-#{jruby_version} /tmp/#{pkg_name}/jruby"
    system "rm /tmp/#{pkg_name}/jruby.tar.gz"
    system %[
      cd /tmp/#{pkg_name}
      export PATH=`pwd`/jruby/bin:$PATH
      unset GEM_HOME
      unset GEM_PATH
      gem install activerecord -v 4.2.8
      gem install jdbc-mysql -v 5.1.42
      gem install jdbc-postgres -v 9.4.1206
      gem install activerecord-jdbcmysql-adapter -v 1.3.23
      gem install activerecord-jdbcpostgresql-adapter -v 1.3.23
      gem install awesome_print -v 1.7.0
    ]
    system "cd /tmp; rm -f #{pkg_name}.zip; zip -r #{pkg_name}.zip #{pkg_name} >/dev/null"
    system "mkdir -p pkg"
    system "cp /tmp/#{pkg_name}.zip pkg"
  end
end