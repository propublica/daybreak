# -*- encoding: utf-8 -*-
require File.expand_path('../lib/daybreak/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Jeff Larson", "Daniel Mendler"]
  gem.email         = ["thejefflarson@gmail.com", "mail@daniel-mendler.de"]
  gem.description   = %q{Incredibly fast pure-ruby key-value store}
  gem.summary       = %q{Daybreak provides an incredibly fast pure-ruby in memory key-value store, which is multi-process safe and uses a journal log to store the data.}
  gem.homepage      = "http://propublica.github.com/daybreak/"

  gem.files         = `git ls-files`.split($\).reject {|f| f =~ /^(index)/}
  gem.test_files    = gem.files.grep(%r{^(test)/})
  gem.name          = "daybreak"
  gem.require_paths = ["lib"]
  gem.licenses      = ["MIT"]
  gem.version       = Daybreak::VERSION
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'minitest'
end
