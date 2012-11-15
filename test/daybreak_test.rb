require 'rubygems'
require 'simplecov'
require 'set'
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
    db2.close!
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

  it "should be able to sync competing writes" do
    @db.set('1', 4, true)
    db2 = Daybreak::DB.new DB_PATH
    db2.set('1', 5, true)
    @db.read!
    assert_equal @db['1'], 5
    @db.close!
  end

  it "should be able to handle another process's call to compact" do
    20.times {|i| @db.set i, i, true }
    db2 = Daybreak::DB.new DB_PATH
    20.times {|i| @db.set i, i + 1, true }
    @db.compact!
    db2.read!
    assert_equal 20, db2['19']
  end

  it "should handle default values that are procs" do
    db = Daybreak::DB.new(DB_PATH) {|key| Set.new }
    assert db['foo'].is_a? Set
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
    n.times {|i| assert_equal i % 1000, @db[i % 1000] }
  end

  after do
    @db.empty!
    @db.close!
    File.unlink(DB_PATH)
  end
end

require 'pstore'

describe "compare with pstore" do
  before do
    @pstore = PStore.new(File.join(HERE, "test.pstore"))
  end

  bench_performance_constant "pstore bulk performance" do |n|
    @pstore.transaction do
      n.times do |i|
        @pstore[i] = 'i' * i
      end
    end
  end

  after do
    File.unlink File.join(HERE, "test.pstore")
  end
end

require 'dbm'

describe "compare with dbm" do
  before do
    @dbm = DBM.open(File.join(HERE, "test-dbm"), 666, DBM::WRCREAT)
    1000.times {|i| @dbm[i.to_s] = i }
  end

  bench_performance_constant "DBM write performance" do |n|
    n.times do |i|
      @dbm[i.to_s] = 'i' * i
    end
  end

  bench_performance_constant "DBM read performance" do |n|
    n.times do |i|
      assert_equal (i % 1000).to_s, @dbm[(i % 1000).to_s]
    end
  end

  after do
    @dbm.close

    File.unlink File.join(HERE, "test-dbm.db")
  end
end
