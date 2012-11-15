require 'minitest/autorun'
require 'minitest/benchmark'

HERE = File.expand_path(File.dirname(__FILE__))
DB_PATH = File.join HERE, "test.db"

require File.join HERE, '..', 'lib', 'daybreak'