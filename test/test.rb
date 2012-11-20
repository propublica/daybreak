require 'rubygems'
require 'simplecov'
require 'set'
SimpleCov.start
SimpleCov.command_name "Unit tests"

require File.expand_path(File.dirname(__FILE__)) + '/test_helper.rb'

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

  it "should handle default values that are procs" do
    db = Daybreak::DB.new(DB_PATH) {|key| Set.new }
    assert db['foo'].is_a? Set
  end

  it "should be able to sync competing writes" do
    @db.set! '1', 4
    db2 = Daybreak::DB.new DB_PATH
    db2.set! '1', 5
    @db.read!
    assert_equal @db['1'], 5
    @db.close!
  end

  it " should be able to handle another process's call to compact" do
    20.times {|i| @db.set i, i, true }
    db2 = Daybreak::DB.new DB_PATH
    20.times {|i| @db.set i, i + 1, true }
    @db.compact!
    db2.read!
    assert_equal 20, db2['19']
  end

  after do
    @db.empty!
    @db.close!
    File.unlink(DB_PATH)
  end
end


