require File.expand_path(File.dirname(__FILE__)) + '/test_helper.rb'
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
