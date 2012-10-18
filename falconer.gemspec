# -*- encoding: utf-8 -*-
require File.expand_path('../lib/falconer/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jeff Larson"]
  gem.email         = ["thejefflarson@gmail.com"]
  gem.description   = %q{A simple dimple key-value store for ruby.}
  gem.summary       = %q{Falconer provides an in memory key-value store that is easily enumerable in ruby.}
  gem.homepage      = "http://propublica.github.com/falconer/"

  gem.files         = `git ls-files`.split($\)
  gem.test_files    = gem.files.grep(%r{^(test)/})
  gem.name          = "TK"
  gem.require_paths = ["lib"]
  gem.version       = Falconer::VERSION
end
