module Falconer
  ROOT = File.expand_path(File.dirname(__FILE__))
end

require 'tempfile'
require 'thread'
require 'zlib'
require "#{Falconer::ROOT}/falconer/version"
require "#{Falconer::ROOT}/falconer/locking"
require "#{Falconer::ROOT}/falconer/record"
require "#{Falconer::ROOT}/falconer/writer"
require "#{Falconer::ROOT}/falconer/reader"
require "#{Falconer::ROOT}/falconer/db"
