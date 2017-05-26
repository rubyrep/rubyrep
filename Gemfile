source 'http://rubygems.org'

gem 'activerecord', '~> 4.2'
platforms :ruby do
  gem 'pg'
  gem 'mysql2'
end
platforms :jruby do
  gem 'activerecord-jdbc-adapter', :git => "https://github.com/jruby/activerecord-jdbc-adapter.git", branch: "rails-5"
  gem 'activerecord-jdbcpostgresql-adapter', :git => "https://github.com/jruby/activerecord-jdbc-adapter.git", branch: "rails-5"
  gem 'activerecord-jdbcmysql-adapter', :git => "https://github.com/jruby/activerecord-jdbc-adapter.git", branch: "rails-5"
end
gem 'rspec'
gem 'crack'
gem 'awesome_print', require: 'ap'
gem 'rake'
gem 'simplecov', :require => false, :group => :test