module Daybreak
  ROOT = File.expand_path(File.dirname(__FILE__))
end

require 'tempfile'
require 'thread'
require 'zlib'
require "#{Daybreak::ROOT}/daybreak/version"
require "#{Daybreak::ROOT}/daybreak/locking"
require "#{Daybreak::ROOT}/daybreak/record"
require "#{Daybreak::ROOT}/daybreak/writer"
require "#{Daybreak::ROOT}/daybreak/reader"
require "#{Daybreak::ROOT}/daybreak/db"
