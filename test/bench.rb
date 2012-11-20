require File.expand_path(File.dirname(__FILE__)) + '/test_helper.rb'

describe "benchmarks" do
  before do
    @db = Daybreak::DB.new DB_PATH
    1000.times {|i| @db[i] = i }
    @db.flush!
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

