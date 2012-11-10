module Daybreak
  # Daybreak::DB contains the public api for Daybreak, you may extend it like
  # any other Ruby class (i.e. to overwrite serialize and parse). It includes
  # Enumerable for functional goodies like map, each, reduce and friends.
  class DB
    include Enumerable

    # Create a new Daybreak::DB. The second argument is the default value
    # to store when accessing a previously unset key, this follows the
    # Hash standard.
    def initialize(file, default=nil)
      @file_name = file
      reset!
      @default   = default
      read!
    end

    # Set a key in the database to be written at some future date. If the data
    # needs to be persisted immediately, call db.set(key, value, true).
    def []=(key, value, sync = false)
      key = key.to_s
      @writer.write(Record.new(key, serialize(value)))
      flush! if sync
      @table[key] = value
    end
    alias_method :set, :"[]="

    # Retrieve a value at key from the database. If the default value was specified
    # when this database was created, that value will be set and returned.
    def [](key)
      key = key.to_s
      if @table.has_key? key
        @table[key]
      elsif default?
        set key, @default
      end
    end
    alias_method :get, :"[]"

    # Iterate over the key, value pairs in the database.
    def each(&blk)
      keys.each { |k| blk.call(k, get(k)) }
    end

    # Does this db have a default value.
    def default?
      !@default.nil?
    end

    # Does this db have a value for this key?
    def has_key?(key)
      @table.has_key? key.to_s
    end

    # Return the keys in the db;
    def keys
      @table.keys
    end

    # Return the number of stored items.
    def length
      @table.keys.length
    end

    # Serialize the data for writing to disk, if you don't want to use <tt>Marshal</tt>
    # overwrite this method.
    def serialize(value)
      Marshal.dump(value)
    end

    # Parse the serialized value from disk, like serialize if you want to use a
    # different serialization method overwrite this method.
    def parse(value)
      Marshal.load(value)
    end

    # Reset and empty the database file.
    def empty!
      reset!
      @writer.truncate!
    end

    # Force all queued commits to be written to disk.
    def flush!
      @writer.flush!
    end

    # Reset the state of the database, you should call <tt>read!</tt> after calling this.
    def reset!
      @table  = {}
      @writer = Daybreak::Writer.new(@file_name)
      @reader = Daybreak::Reader.new(@file_name)
    end

    def close!
      @writer.close!
      @reader.close!
    end

    def compact!
      tmp_file = Tempfile.new File.basename(@file_name)
      copy_db  = DB.new tmp_file.path

      each do |key, i|
        copy_db.set(key, get(key))
      end
      copy_db.close!

      empty!

      tmp_file.close
      FileUtils.mv tmp_file.path, @file_name
      tmp_file.unlink

      close!
      reset!
      read!
    end

    def read!
      @reader.read do |record|
        @table[record.key] = parse record.data
      end
    end
  end
end
