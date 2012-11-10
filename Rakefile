#!/usr/bin/env rake
require "bundler/gem_tasks"

task :default do
  require "./test/daybreak_test.rb"
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
