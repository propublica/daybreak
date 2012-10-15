# -*- encoding: utf-8 -*-
require File.expand_path('../lib/falconer/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jeff Larson"]
  gem.email         = ["thejefflarson@gmail.com"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "falconer"
  gem.require_paths = ["lib"]
  gem.version       = Falconer::VERSION
end
