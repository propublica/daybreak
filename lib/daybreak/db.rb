module Daybreak
  # Daybreak::DB contains the public api for Daybreak. It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  class DB
    include Enumerable

    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    # @param [String] file the path to the db file
    # @param default the default value to store and return when a key is
    #  not yet in the database.
    # @yield [key] a block that will return the default value to store.
    # @yieldparam [String] key the key to be stored.
    def initialize(file, default = nil, serializer = Serializer, &block)
      @file, @serializer = file, serializer.new
      @default = block ? block : default
      @out = File.open(@file, 'ab')
      @queue = Queue.new
      @mutex = Mutex.new
      @flush = ConditionVariable.new
      reset
      @thread = Thread.new(&method(:worker))
      at_exit(&method(:finish))
      sync
    end

    def [](key)
      skey = @serializer.key_for(key)
      if @table.has_key?(skey)
        @table[skey]
      elsif @default
        set(key, @default.respond_to?(:call) ? @default.call(key) : @default)
      end
    end
    alias_method :get, :'[]'

    def []=(key, value)
      key = @serializer.key_for(key)
      @queue << [key, value]
      @table[key] = value
    end
    alias_method :set, :'[]='

    def set!(key, value)
      set(key, value)
      flush
      value
    end

    def delete(key)
      key = @serializer.key_for(key)
      @queue << [key]
      @table.delete(key)
    end

    def has_key?(key)
      @table.has_key?(@serializer.key_for(key))
    end

    # Does this db have a default value.
    def default?
      !@default.nil?
    end

    def size
      @table.size
    end
    alias_method :length, :size

    def each(&block)
      @table.each(&block)
    end

    def keys
      @table.keys
    end

    def sync
      @mutex.synchronize do
        flush
        update(true)
      end
    end

    def lock
      @mutex.synchronize do
        exclusive do
          flush
          update(false)
          yield
          flush
        end
      end
    end

    def clear
      @mutex.synchronize do
        exclusive do
          flush
          @out.truncate(0)
          @out.pos = @size = 0
          @table.clear
        end
      end
    end

    def compact
      tmpfile = "#{@file}-#{$$}-#{Thread.current.object_id}"
      tmp = File.open(tmpfile, 'wb')
      @mutex.synchronize do
        compactsize = tmp.write(dump)
        exclusive do
          newsize = @in.stat.size
          # Is the new database smaller than the old one?
          if newsize != compactsize
            # Check if database changed in the meantime
            if newsize > @size
              @in.pos = @size
              # Append changed journal entries
              tmp.write(@in.read(newsize - @size))
            end
            tmp.close
            File.rename(tmpfile, @file)
          end
        end
      end
    ensure
      tmp.close unless tmp.closed?
      File.unlink(tmpfile) if File.exists? tmpfile
    end

    def close
      finish
      @in.close
      @out.close
    end

    private

    def finish
      @queue << nil
      @thread.join
    end

    def update(lock)
      buf = ''
      begin
        stat = nil
        loop do
          @in.flock(File::LOCK_SH) if lock
          stat = @in.stat
          # Check if database was compactified in the meantime
          # break if not
          break if stat.nlink > 0
          @in.close
          reset
        end

        # Read new journal entries
        if stat.size > @size
          @in.pos = @size
          buf = @in.read(stat.size - @size)
          @size = stat.size
        end
      ensure
        @in.flock(File::LOCK_UN) if lock
      end

      until buf.empty?
        record = @serializer.deserialize(buf)
        if record.size == 1
          @table.delete(record.first)
        else
          @table[record.first] = record.last
        end
      end
    end

    def flush
      @flush.wait(@mutex)
    end

    def reset
      @in = File.open(@file, 'rb')
      @size = 0
      @table = {}
    end

    def dump
      @table.inject('') do |dump, record|
        dump << @serializer.serialize(record)
      end
    end

    def worker
      loop do
        @flush.signal if @queue.empty?

        record = @queue.pop || break

        record = @serializer.serialize(record)
        @mutex.synchronize do
          exclusive do
            @out.write(record)
            # Flush to make sure the file is really updated
            @out.flush
            size = @out.stat.size
          end
          @size = size if size == @size + record.size
        end
      end
    end

    def exclusive
      loop do
        @out.flock(File::LOCK_EX)
        # Check if database was compactified in the meantime
        # break if not
        break if @out.stat.nlink > 0
        @out.close
        @out = File.open(@file, 'ab')
      end
      yield
    ensure
      @out.flock(File::LOCK_UN)
    end
  end
end
