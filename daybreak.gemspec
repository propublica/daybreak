# -*- encoding: utf-8 -*-
require File.expand_path('../lib/daybreak/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jeff Larson"]
  gem.email         = ["thejefflarson@gmail.com"]
  gem.description   = %q{A simple dimple key-value store for ruby.}
  gem.summary       = %q{Daybreak provides an in memory key-value store that is easily enumerable in ruby.}
  gem.homepage      = "http://propublica.github.com/daybreak/"

  gem.files         = `git ls-files`.split($\)
  gem.test_files    = gem.files.grep(%r{^(test)/})
  gem.name          = "daybreak"
  gem.require_paths = ["lib"]
  gem.version       = Daybreak::VERSION
end
