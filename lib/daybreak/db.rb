module Daybreak
  # Daybreak::DB contains the public api for Daybreak. It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  # @api public
  class DB
    include Enumerable

    attr_reader :file, :logsize
    attr_writer :default

    @databases = []
    @databases_mutex = Mutex.new

    at_exit do
      loop do
        db = @databases_mutex.synchronize { @databases.first }
        break unless db
        warn "Daybreak database #{db.file} was not closed, state might be inconsistent"
        begin
          db.close
        rescue Exception => ex
          warn "Failed to close daybreak database: #{ex.message}"
        end
      end
    end

    class << self
      # @api private
      def register(db)
        @databases_mutex.synchronize { @databases << db }
      end

      # @api private
      def unregister(db)
        @databases_mutex.synchronize { @databases.delete(db) }
      end
    end

    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    # @param [String] file the path to the db file
    # @param [Hash] options a hash that contains the options for creating a new
    #  database. You can pass in :serializer, :format or :default.
    # @yield [key] a block that will return the default value to store.
    # @yieldparam [String] key the key to be stored.
    def initialize(file, options = {}, &block)
      @file = file
      @serializer = (options[:serializer] || Serializer::Default).new
      @format = (options[:format] || Format).new(@serializer)
      @default = block ? block : options[:default]
      @queue = Queue.new
      @table = {}
      reopen
      @worker = Thread.new(&method(:worker))
      @mutex = Mutex.new # a global mutex for lock
      sync
      self.class.register(self)
    end

    # Return default value belonging to key
    # @param key the default value to retrieve.
    def default(key = nil)
      @default.respond_to?(:call) ? @default.call(key) : @default
    end

    # Retrieve a value at key from the database. If the default value was specified
    # when this database was created, that value will be set and returned. Aliased
    # as <tt>get</tt>.
    # @param key the value to retrieve from the database.
    def [](key)
      skey = @serializer.key_for(key)
      if @table.has_key?(skey)
        @table[skey]
      elsif @default
        value = default(key)
        @queue << [skey, value]
        @table[skey] = value
      end
    end
    alias_method :get, :'[]'

    # Set a key in the database to be written at some future date. If the data
    # needs to be persisted immediately, call <tt>db.set(key, value, true)</tt>.
    # @param [#to_s] key the key of the storage slot in the database
    # @param value the value to store
    def []=(key, value)
      key = @serializer.key_for(key)
      @queue << [key, value]
      @table[key] = value
    end
    alias_method :set, :'[]='

    # set! flushes data immediately to disk.
    # @param key the key of the storage slot in the database
    # @param value the value to store
    def set!(key, value)
      set(key, value)
      flush
      value
    end

    # Delete a key from the database
    # @param key the key of the storage slot in the database
    def delete(key)
      key = @serializer.key_for(key)
      @queue << [key]
      @table.delete(key)
    end

    # delete! immediately deletes the key on disk.
    # @param key the key of the storage slot in the database
    def delete!(key)
      value = delete(key)
      flush
      value
    end

    # Does this db have a value for this key?
    # @param key the key to check if the DB has a key.
    def has_key?(key)
      @table.has_key?(@serializer.key_for(key))
    end
    alias_method :key?, :has_key?
    alias_method :include?, :has_key?
    alias_method :member?, :has_key?

    def has_value?(value)
      @table.has_value?(value)
    end
    alias_method :value?, :has_value?

    # Return the number of stored items.
    # @return [Integer]
    def size
      @table.size
    end
    alias_method :length, :size

    # Return true if database is empty
    # @return [Boolean]
    def empty?
      @table.empty?
    end

    # Iterate over the key, value pairs in the database.
    # @yield [key, value] blk the iterator for each key value pair.
    # @yieldparam key the key.
    # @yieldparam value the value from the database.
    def each(&block)
      @table.each(&block)
    end

    # Return the keys in the db.
    # @return [Array]
    def keys
      @table.keys
    end

    # Flush all changes
    def flush
      @queue.flush
    end

    # Sync the database with what is on disk, by first flushing changes, and
    # then reading the file if necessary.
    def sync
      flush
      buf = new_records
      until buf.empty?
        record = @format.deserialize(buf)
        if record.size == 1
          @table.delete(record.first)
        else
          @table[record.first] = record.last
        end
        @logsize += 1
      end
    end

    # Lock the database for an exclusive commit accross processes and threads
    # @yield a block where every change to the database is synced
    def lock
      @mutex.synchronize do
        # We need a flush before exclusive
        # so that @exclusive is not modified by the worker
        flush
        exclusive do
          sync
          result = yield
          flush
          result
        end
      end
    end

    # Remove all keys and values from the database
    def clear
      flush
      with_tmpfile do |path, file|
        file.write(@format.header)
        file.close
        # Clear acts like a compactification
        File.rename(path, @file)
      end
      @table.clear
      reopen
      self
    end

    # Compact the database to remove stale commits and reduce the file size.
    def compact
      sync
      with_tmpfile do |path, file|
        compactsize = file.write(dump)
        exclusive do
          stat = @in.stat
          # Check if database was compactified at the same time
          if stat.nlink > 0 && stat.ino == @in_ino
            # Compactified database has the same size -> return
            return self if stat.size == compactsize
            # Append changed journal records if the database changed during compactification
            file.write(@in.read)
            file.close
            File.rename(path, @file)
          end
        end
      end
      reopen
      sync
      self
    end

    # Close the database for reading and writing.
    def close
      @queue << nil
      @worker.join
      @in.close
      @out.close
      @queue.stop if @queue.respond_to?(:stop)
      self.class.unregister(self)
      nil
    end

    private

    # Read new records from journal log and return buffer
    def new_records
      loop do
        @in.flock(File::LOCK_SH) unless @exclusive
        # Check if database was compactified in the meantime
        # break if not
        stat = @in.stat
        break if stat.nlink > 0 && stat.ino == @in_ino
        @table.clear
        reopen_in
      end

      # Read new journal records
      @in.read
    ensure
      @in.flock(File::LOCK_UN) unless @exclusive
    end

    # Reopen input
    def reopen_in
      @logsize = 0
      @in.close if @in
      @in = File.open(@file, 'rb')
      @in_ino = @in.stat.ino
      @format.read_header(@in)
    end

    # Reopen output
    def reopen_out
      @out.close if @out
      @out = File.open(@file, 'ab')
      stat = @out.stat
      @out_ino = stat.ino
      if stat.size == 0
        @out.write(@format.header)
        @out.flush
      end
    end

    # Reopen output and input
    def reopen
      reopen_out
      reopen_in
    end

    # Return database dump as string
    def dump
      dump = @format.header
      @table.each do |record|
        dump << @format.serialize(record)
      end
      dump
    end

    # Worker thread
    def worker
      loop do
        record = @queue.next
        write_record(record) if record
        @queue.pop
        break unless record
      end
    rescue Exception => ex
      warn "Daybreak worker: #{ex.message}"
      retry
    end

    # Write record to output stream and
    # advance input stream
    def write_record(record)
      record = @format.serialize(record)
      exclusive do
        @out.write(record)
        # Flush to make sure the file is really updated
        @out.flush
        size = @out.stat.size
      end
      @in.pos = size if size == @in.pos + record.size
      @logsize += 1
    end

    # Lock database exclusively
    def exclusive
      return yield if @exclusive
      begin
        loop do
          @out.flock(File::LOCK_EX)
          # Check if database was compactified in the meantime
          # break if not
          stat = @out.stat
          break if stat.nlink > 0 && stat.ino == @out_ino
          reopen_out
        end
        @exclusive = true
        yield
      ensure
        @out.flock(File::LOCK_UN)
        @exclusive = false
      end
    end

    # Open temporary file and pass it to the block
    def with_tmpfile
      path = [@file, $$.to_s(36), Thread.current.object_id.to_s(36)].join
      file = File.open(path, 'wb')
      yield(path, file)
    ensure
      file.close unless file.closed?
      File.unlink(path) if File.exists?(path)
    end
  end
end
