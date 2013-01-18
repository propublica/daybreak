module Daybreak
  # A queue for threaded implementations of ruby without a GIL
  # @api private
  class Queue
    def initialize
      @mutex = Mutex.new
      @full = ConditionVariable.new
      @empty = ConditionVariable.new
      @queue = []
    end

    def <<(x)
      @mutex.synchronize do
        @queue << x
        @full.signal
      end
    end

    def pop
      @mutex.synchronize do
        @queue.shift
        @empty.signal if @queue.empty?
      end
    end

    def first
      @mutex.synchronize do
        @full.wait(@mutex) while @queue.empty?
        @queue.first
      end
    end

    def flush
      @mutex.synchronize do
        @empty.wait(@mutex) until @queue.empty?
      end
    end

    def close
    end
  end
end
