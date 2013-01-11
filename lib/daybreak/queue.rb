module Daybreak
  class Queue
    def initialize
      @mutex = Mutex.new
      @full = ConditionVariable.new
      @empty = ConditionVariable.new
      @queue = []
    end

    if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'ruby'
      # HACK: Dangerous optimization on MRI which has a
      # global interpreter lock and makes the @queue array
      # thread safe.

      def <<(x)
        @queue << x
        @full.signal
      end

      def pop
        @queue.shift
        @empty.signal if @queue.empty?
      end
    else
      # JRuby and Rubinius don't have a GIL

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
    end

    def next
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
  end
end
