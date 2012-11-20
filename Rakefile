#!/usr/bin/env rake
require "bundler/gem_tasks"

task :default do
  require "./test/test.rb"
end

desc "Run benchmarks"
task :bench do
  require "./test/bench.rb"
end

desc "Run comparisons with other libraries"
task :compare do
  require "./test/compare.rb"
end

desc "Profile a simple run"
task :prof do
  require "./test/prof.rb"
end

require 'erb'

desc "Write out docs to index.html"
task :doc do |t|
  File.open("index.html", 'w').write ERB.new(File.open("index.erb").read).result(binding)
end

desc "Publish the docs to gh-pages"
task :publish do |t|
  `git checkout gh-pages`
  `git merge master`
  `git push`
  `git checkout master`
end
