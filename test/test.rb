require 'set'

# begin
#   require 'simplecov'
#   SimpleCov.start
#   SimpleCov.command_name "Unit tests"
# rescue Exception => ex
#   puts "No coverage report generated: #{ex.message}"
# end

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
    @db['1'] = '4'
    @db['4'] = '1'
    @db.sync

    assert_equal @db['1'], '4'
    db2 = Daybreak::DB.new DB_PATH
    assert_equal db2['1'], '4'
    assert_equal db2['4'], '1'
    db2.close
  end

  it "should compact cleanly" do
    @db[1] = 1
    @db[1] = 1
    @db.sync

    size = File.stat(DB_PATH).size
    @db.compact
    assert_equal @db[1], 1
    assert size > File.stat(DB_PATH).size
  end

  it "should allow for default values" do
    default_db = Daybreak::DB.new(DB_PATH, :default => 0)
    assert_equal default_db[1], 0
    default_db[1] = 1
    assert_equal default_db[1], 1
    default_db.close
  end

  it "should handle default values that are procs" do
    db = Daybreak::DB.new(DB_PATH) {|key| Set.new }
    assert db['foo'].is_a? Set
    db.close
  end

  it "should be able to sync competing writes" do
    @db.set! '1', 4
    db2 = Daybreak::DB.new DB_PATH
    db2.set! '1', 5
    @db.sync
    assert_equal @db['1'], 5
    db2.close
  end

  it "should be able to handle another process's call to compact" do
    @db.lock { 20.times {|i| @db[i] = i } }
    db2 = Daybreak::DB.new DB_PATH
    @db.lock { 20.times {|i| @db[i] = i } }
    @db.compact
    db2.sync
    assert_equal 19, db2['19']
    db2.close
  end

  it "can empty the database" do
    20.times {|i| @db[i] = i }
    @db.clear
    db2 = Daybreak::DB.new DB_PATH
    assert_equal nil, db2['19']
    db2.close
  end

  it "should handle deletions" do
    @db[1] = 'one'
    @db[2] = 'two'
    @db.delete! 'two'
    assert !@db.has_key?('two')
    assert_equal @db['two'], nil

    db2 = Daybreak::DB.new DB_PATH
    assert !db2.has_key?('two')
    assert_equal db2['two'], nil
    db2.close
  end

  it "should close and reopen the file when clearing the database" do
    begin
      1000.times {@db.clear}
    rescue
      flunk
    end
  end


  it "should be threadsafe" do
    @db[1] = 0
    inc = Proc.new { 1000.times { @db.lock { @db[1] += 1 } } }
    a = Thread.new &inc
    b = Thread.new &inc
    a.join
    b.join
    assert_equal @db[1], 2000
  end

  it "should synchonize across processes" do
    @db[1] = 0
    @db.sync
    @db.close
    inc = Proc.new do
      db = Daybreak::DB.new DB_PATH
      1000.times { db.lock { db[1] += 1  } }
      db.close
    end
    a = fork &inc
    b = fork &inc
    Process.wait a
    Process.wait b
    @db = Daybreak::DB.new DB_PATH
    assert_equal @db[1], 2000
  end

  after do
    @db.clear
    @db.close
  end
end
