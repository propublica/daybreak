require 'thread'
require 'zlib'

module Daybreak
  class DB
    include Enumerable

    def initialize(file)
      @file = file
      @out = File.open(@file, 'ab')
      @queue = Queue.new
      @mutex = Mutex.new
      @flush = ConditionVariable.new
      reset
      @thread = Thread.new(&method(:worker))
      sync
    end

    def [](key)
      @table[key]
    end

    def []=(key, value)
      @queue << [key, value]
      @table[key] = value
    end

    def delete(key)
      @queue << [key]
      @table.delete(key)
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
      @queue << nil
      @thread.join
      @in.close
      @out.close
    end

    private

    DELETE = (1 << 32) - 1

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
        key, value = deserialize(buf)
        if value == nil
          @table.delete(key)
        else
          @table[key] = value
        end
      end
    end

    def reset
      @in = File.open(@file, 'rb')
      @size = 0
      @table = {}
    end

    def dump
      dump = ''
      @table.each do |key, value|
        dump << serialize([key, value, false])
      end
      dump
    end

    def deserialize(buf)
      key_size, value_size = buf[0, 8].unpack('NN')
      if value_size == DELETE
        data = buf.slice!(0, 8 + key_size)
        value = nil
      else
        data = buf.slice!(0, 8 + key_size + value_size)
        value = data[8 + key_size, value_size]
      end
      raise 'CRC mismatch' unless buf.slice!(0, 4) == crc32(data)
      [data[8, key_size], value]
    end

    def serialize(record)
      raise 'Key must be a string' unless String === record[0]
      data =
        if record[1] == nil
          [record[0].bytesize, DELETE].pack('NN') << record[0]
        else
          raise 'Value must be a string' unless String === record[1]
          [record[0].bytesize, record[1].bytesize].pack('NN') << record[0] << record[1]
        end
      data << crc32(data)
    end

    def crc32(s)
      [Zlib.crc32(s, 0)].pack('N')
    end

    def worker
      loop do
        @flush.signal if @queue.empty?
        record = @queue.pop || break
        record = serialize(record)
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
    rescue Exception => ex
      warn "Database worker: #{ex.message}"
      retry
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
