require 'rubygems'
require 'simplecov'
SimpleCov.start
SimpleCov.command_name "Unit tests"

require 'minitest/autorun'
require 'minitest/benchmark'

HERE = File.expand_path(File.dirname(__FILE__))
DB_PATH = File.join HERE, "test.db"

require File.join HERE, '..', 'lib', 'daybreak'

describe "database functions" do
  before do
    @db = Daybreak::DB.new DB_PATH
  end

  it "should insert" do
    @db[1] = 1
    assert_equal @db[1], 1
    assert @db.has_key?(1)
    @db[1] = '2'
    assert_equal @db[1], '2'
    assert_equal @db.length, 1
  end

  it "should persist values" do
    @db.set('1', '4', true)
    @db.set('4', '1', true)

    assert_equal @db['1'], '4'
    db2 = Daybreak::DB.new DB_PATH
    assert_equal db2['1'], '4'
    assert_equal db2['4'], '1'
  end

  it "should compact cleanly" do
    @db[1] = 1
    @db[1] = 1
    @db.flush!
    size = File.stat(DB_PATH).size
    @db.compact!
    assert_equal @db[1], 1
    assert size > File.stat(DB_PATH).size
  end

  it "should allow for default values" do
    default_db = Daybreak::DB.new(DB_PATH, 0)
    assert_equal default_db[1], 0
    default_db[1] = 1
    assert_equal default_db[1], 1
  end

  after do
    @db.empty!
    @db.close!
    File.unlink(DB_PATH)
  end
end

describe "benchmarks" do
  before do
    @db = Daybreak::DB.new DB_PATH
    1000.times {|i| @db[i] = i }
    @db.flush!
    @db = Daybreak::DB.new DB_PATH
  end

  bench_performance_constant "keys with sync" do |n|
    n.times {|i| @db.set(i, 'i' * i, true) }
  end

  bench_performance_constant "inserting keys" do |n|
    n.times {|i| @db[i] = 'i' * i }
  end

  bench_performance_constant "reading keys" do |n|
    n.times {|i|
      assert_equal i % 1000, @db[i % 1000]
    }
  end

  after do
    @db.empty!
    @db.close!
    File.unlink(DB_PATH)
  end
end
