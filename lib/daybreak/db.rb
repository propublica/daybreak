module Daybreak
  # Daybreak::DB contains the public api for Daybreak, you may extend it like
  # any other Ruby class (i.e. to overwrite serialize and parse). It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  class DB
    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    # @param [String] file the path to the db file
    # @param default the default value to store and return when a key is
    #  not yet in the database.
    # @yield [key] a block that will return the default value to store.
    # @yieldparam [String] key the key to be stored.
    include Enumerable

    def initialize(file, default = nil)
      @file = file
      @out = File.open(@file, 'ab')
      @queue = Queue.new
      @mutex = Mutex.new
      @flush = ConditionVariable.new
      reset
      @thread = Thread.new(&method(:worker))
      at_exit { finish }
      sync
    end

    def [](key)
      key = key.to_s
      @table[key]
    end
    alias_method :get, :"[]"

    def []=(key, value)
      key = key.to_s
      @queue << [key.to_s, serialize(value)]
      @table[key] = value
    end
    alias_method :set, :"[]="

    def delete(key)
      key = key.to_s
      @queue << [key]
      @table.delete(ke)
    end

    def has_key?(key)
      @table.has_key?(key)
    end

    def size
      @table.size
    end

    def each(&block)
      @table.each(&block)
    end

    def keys
      @table.keys
    end

    def sync
      @mutex.synchronize do
        @flush.wait(@mutex)
        update(true)
      end
    end

    def lock
      @mutex.synchronize do
        exclusive do
          @flush.wait(@mutex)
          update(false)
          yield
          @flush.wait(@mutex)
        end
      end
    end

    def clear
      @mutex.synchronize do
        exclusive do
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
      File.unlink(tmpfile)
    end

    def close
      finish
      @in.close
      @out.close
    end

    # Serialize the data for writing to disk, if you don't want to use <tt>Marshal</tt>
    # overwrite this method.
    # @param value the value to be serialized
    # @return [String]
    def serialize(value)
      Marshal.dump(value)
    end

    # Parse the serialized value from disk, like serialize if you want to use a
    # different serialization method overwrite this method.
    # @param value the value to be parsed
    # @return [String]
    def parse(value)
      Marshal.load(value)
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
        key, value = Record.deserialize(buf)
        if value == nil
          @table.delete(key)
        else
          @table[key] = parse(value)
        end
      end
    end

    def reset
      @in = File.open(@file, 'rb')
      @size = 0
      @table = {}
    end

    def dump
      @table.reduce('') do |dump, key, value|
        dump << Record.serialize([key, serialize(value), false])
      end
    end

    def worker
      loop do
        @flush.signal if @queue.empty?

        record = @queue.pop || break

        record = Record.serialize(record)
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
