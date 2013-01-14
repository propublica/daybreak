#!/usr/bin/env rake
require "bundler/gem_tasks"

task :test do
  ruby 'test/test.rb'
end

desc "Run benchmarks"
task :bench do
  ruby 'script/bench'
end

desc "Profile a simple run"
task :prof do
  ruby 'test/prof.rb'
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

task :default => :test
