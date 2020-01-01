# encoding: utf-8
require 'minitest/autorun'
require 'minitest/benchmark'

require 'set'

require File.expand_path(File.dirname(__FILE__)) + '/test_helper.rb'

describe Daybreak::DB do
  before do
    @db = Daybreak::DB.new DB_PATH
  end

  it 'should insert' do
    assert_nil @db[1]
    assert_equal @db.include?(1), false
    @db[1] = 1
    assert_equal @db[1], 1
    assert @db.has_key?(1)
    @db[1] = '2'
    assert_equal @db[1], '2'
    assert_equal @db.length, 1
  end

  it 'should support frozen key' do
    key = 'key'
    key.freeze
    @db[key] = 'value'
    assert_equal @db[key], 'value'
  end

  it 'should support batch inserts' do
    @db.update(1 => :a, 2 => :b)
    assert_equal @db[1], :a
    assert_equal @db[2], :b
    assert_equal @db.length, 2
  end

  it 'should persist values' do
    @db['1'] = '4'
    @db['4'] = '1'
    assert_equal @db.sunrise, @db

    assert_equal @db['1'], '4'
    db = Daybreak::DB.new DB_PATH
    assert_equal db['1'], '4'
    assert_equal db['4'], '1'
    assert_nil db.close
  end

  it 'should persist after batch update' do
    @db.update!(1 => :a, 2 => :b)

    db = Daybreak::DB.new DB_PATH
    assert_equal db[1], :a
    assert_equal db[2], :b
    assert_nil db.close
  end

  it 'should persist after clear' do
    @db['1'] = 'xy'
    assert_equal @db.clear, @db
    @db['1'] = '4'
    @db['4'] = '1'
    assert_nil @db.close

    @db = Daybreak::DB.new DB_PATH
    assert_equal @db['1'], '4'
    assert_equal @db['4'], '1'
  end

  it 'should persist after compact' do
    @db['1'] = 'xy'
    @db['1'] = 'z'
    assert_equal @db.compact, @db
    @db['1'] = '4'
    @db['4'] = '1'
    assert_nil @db.close

    @db = Daybreak::DB.new DB_PATH
    assert_equal @db['1'], '4'
    assert_equal @db['4'], '1'
  end

  it 'should reload database file in sync after compact' do
    db = Daybreak::DB.new DB_PATH

    @db['1'] = 'xy'
    @db['1'] = 'z'
    assert_equal @db.compact, @db
    @db['1'] = '4'
    @db['4'] = '1'
    assert_equal @db.flush, @db

    db.sunrise
    assert_equal db['1'], '4'
    assert_equal db['4'], '1'
    db.close
  end

  it 'should reload database file in sync after clear' do
    db = Daybreak::DB.new DB_PATH

    @db['1'] = 'xy'
    @db['1'] = 'z'
    @db.clear
    @db['1'] = '4'
    @db['4'] = '1'
    @db.flush

    db.sunrise
    assert_equal db['1'], '4'
    assert_equal db['4'], '1'
    db.close
  end

  it 'should compact cleanly' do
    @db[1] = 1
    @db[1] = 1
    @db.sunrise

    size = File.stat(DB_PATH).size
    @db.compact
    assert_equal @db[1], 1
    assert size > File.stat(DB_PATH).size
  end

  it 'should allow for default values' do
    db = Daybreak::DB.new(DB_PATH, :default => 0)
    assert_equal db.default(1), 0
    assert_equal db[1], 0
    assert db.include? '1'
    db[1] = 1
    assert_equal db[1], 1
    db.default = 42
    assert_equal db['x'], 42
    db.close
  end

  it 'should handle default values that are procs' do
    db = Daybreak::DB.new(DB_PATH) {|key| set = Set.new; set << key }
    assert db.default(:test).include? 'test'
    assert db['foo'].is_a? Set
    assert db.include? 'foo'
    assert db['bar'].include? 'bar'
    db.default = proc {|key| [key] }
    assert db[1].is_a? Array
    assert db[2] == ['2']
    db.close
  end

  it 'should be able to sync competing writes' do
    @db.set! '1', 4
    db = Daybreak::DB.new DB_PATH
    db.set! '1', 5
    @db.sunrise
    assert_equal @db['1'], 5
    db.close
  end

  it 'should be able to handle another process\'s call to compact' do
    @db.lock { 20.times {|i| @db[i] = i } }
    db = Daybreak::DB.new DB_PATH
    @db.lock { 20.times {|i| @db[i] = i } }
    @db.compact
    db.sunrise
    assert_equal 19, db['19']
    db.close
  end

  it 'can empty the database' do
    20.times {|i| @db[i] = i }
    @db.clear
    db = Daybreak::DB.new DB_PATH
    assert_nil db['19']
    db.close
  end

  it 'should handle deletions' do
    @db['one'] = 1
    @db['two'] = 2
    @db.delete! 'two'
    assert !@db.has_key?('two')
    assert_nil @db['two']

    db = Daybreak::DB.new DB_PATH
    assert !db.has_key?('two')
    assert_nil db['two']
    db.close
  end

  it 'should synchronize deletions after compact' do
    @db['one'] = 1
    @db['two'] = 2
    @db.flush
    db = Daybreak::DB.new DB_PATH
    assert db.has_key?('two')
    @db.delete! 'two'
    @db.compact
    db.sunrise
    assert !db.has_key?('two')
    assert_nil db['two']
    db.close
  end

  it 'should close and reopen the file when clearing the database' do
    begin
      1000.times {@db.clear}
    rescue
      flunk
    end
  end

  it 'should have threadsafe lock' do
    @db[1] = 0
    inc = proc { 1000.times { @db.lock {|d| d[1] += 1 } } }
    a = Thread.new &inc
    b = Thread.new &inc
    a.join
    b.join
    assert_equal @db[1], 2000
  end

  it 'should have threadsafe synchronize' do
    @db[1] = 0
    inc = proc { 1000.times { @db.synchronize {|d| d[1] += 1 } } }
    a = Thread.new &inc
    b = Thread.new &inc
    a.join
    b.join
    assert_equal @db[1], 2000
  end

  it 'should synchronize across processes' do
    @db[1] = 0
    @db.flush
    @db.close
    begin
      a = fork do
        db = Daybreak::DB.new DB_PATH
        1000.times do |i|
          db.lock { db[1] += 1 }
          db["a#{i}"] = i
          sleep 0.01 if i % 100 == 0
        end
        db.close
      end
      b = fork do
        db = Daybreak::DB.new DB_PATH
        1000.times do |i|
          db.lock { db[1] += 1 }
          db["b#{i}"] = i
          sleep 0.01 if i % 100 == 0
        end
        db.close
      end
      Process.wait a
      Process.wait b
      @db = Daybreak::DB.new DB_PATH
      1000.times do |i|
        assert_equal @db["a#{i}"], i
        assert_equal @db["b#{i}"], i
      end
      assert_equal @db[1], 2000
    rescue NotImplementedError
      warn 'fork is not available: skipping multiprocess test'
      @db = Daybreak::DB.new DB_PATH
    end
  end

  it 'should synchronize across threads' do
    @db[1] = 0
    @db.flush
    @db.close
    a = Thread.new do
      db = Daybreak::DB.new DB_PATH
      1000.times do |i|
        db.lock { db[1] += 1 }
        db["a#{i}"] = i
        sleep 0.01 if i % 100 == 0
      end
      db.close
    end
    b = Thread.new do
      db = Daybreak::DB.new DB_PATH
      1000.times do |i|
        db.lock { db[1] += 1 }
        db["b#{i}"] = i
        sleep 0.01 if i % 100 == 0
      end
      db.close
    end
    a.join
    b.join
    @db = Daybreak::DB.new DB_PATH
    1000.times do |i|
      assert_equal @db["a#{i}"], i
      assert_equal @db["b#{i}"], i
    end
    assert_equal @db[1], 2000
  end

  it 'should support background compaction' do
    @db[1] = 0
    @db.flush
    @db.close
    stop = false
    a = Thread.new do
      db = Daybreak::DB.new DB_PATH
      1000.times do |i|
        db.lock { db[1] += 1 }
        db["a#{i}"] = i
        sleep 0.01 if i % 100 == 0
      end
      db.close
    end
    b = Thread.new do
      db = Daybreak::DB.new DB_PATH
      1000.times do |i|
        db.lock { db[1] += 1 }
        db["b#{i}"] = i
        sleep 0.01 if i % 100 == 0
      end
      db.close
    end
    c = Thread.new do
      db = Daybreak::DB.new DB_PATH
      db.compact until stop
      db.close
    end
    d = Thread.new do
      db = Daybreak::DB.new DB_PATH
      db.compact until stop
      db.close
    end
    stop = true
    a.join
    b.join
    c.join
    d.join
    @db = Daybreak::DB.new DB_PATH
    1000.times do |i|
      assert_equal @db["a#{i}"], i
      assert_equal @db["b#{i}"], i
    end
    assert_equal @db[1], 2000
  end

  it 'should support compact in lock' do
    @db[1] = 2
    @db.lock do
      @db[1] = 2
      @db.compact
    end
  end

  it 'should support clear in lock' do
    @db[1] = 2
    @db.lock do
      @db[1] = 2
      @db.clear
    end
  end

  it 'should support flush in lock' do
    @db[1] = 2
    @db.lock do
      @db[1] = 2
      @db.flush
    end
  end

  it 'should support set! and delete! in lock' do
    @db[1] = 2
    @db.lock do
      @db.set!(1, 2)
      @db.delete!(1)
    end
  end

  it 'should allow for inheritance' do
    class Subclassed < Daybreak::DB
      def increment(key, amount = 1)
        lock { self[key] += amount }
      end
    end

    db = Subclassed.new DB_PATH
    db[1] = 1
    assert_equal db.increment(1), 2
    db.clear
    db.close
  end

  it 'should report the bytesize' do
    assert @db.bytesize > 0
  end

  it 'should accept utf-8 keys' do
    @db['🌎'] = '🌎'
    @db.flush
    db = Daybreak::DB.new DB_PATH
    assert_equal db['🌎'], '🌎'
    db.close
  end

  it 'should keep the same format' do
    db = Daybreak::DB.new DB_PATH, :serializer => Daybreak::Serializer::None
    10.times {|i| db[(i+1000).to_s] = i.to_s }
    db.close
    assert_equal File.read(DB_PATH), File.read(File.join(HERE, 'mock.db-test'))
  end

  after do
    @db.clear
    @db.close
  end
end
