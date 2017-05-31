# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'rubyrep/version'

Gem::Specification.new do |spec|
  spec.name          = "rubyrep"
  spec.version       = RR::VERSION
  spec.authors       = ["Arndt Lehmann"]
  spec.email         = ["arndtlehmann@arndtlehmann.com"]

  spec.summary       = %q{Open-source solution for asynchronous, master-master replication of relational databases.}
  spec.homepage      = "http://www.rubyrep.org"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "bin"
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.14"
  spec.add_development_dependency "rake", "~> 12.0"
  spec.add_development_dependency "rspec", "~> 3.5"

  spec.add_runtime_dependency "activerecord", "~> 4.2"
end
