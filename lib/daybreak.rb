# Daybreak, a simple dimple key value store for ruby.
module Daybreak
  # The root path for Daybreak
  ROOT = File.expand_path(File.dirname(__FILE__))
end

require 'tempfile'
require 'thread'
require 'zlib'
require 'fcntl'

require "#{Daybreak::ROOT}/daybreak/version"
require "#{Daybreak::ROOT}/daybreak/locking"
require "#{Daybreak::ROOT}/daybreak/record"
require "#{Daybreak::ROOT}/daybreak/writer"
require "#{Daybreak::ROOT}/daybreak/reader"
require "#{Daybreak::ROOT}/daybreak/db"
