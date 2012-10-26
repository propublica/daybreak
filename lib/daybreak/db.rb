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
      read_all!
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

    def each(&blk)
      keys.each { |k| blk.call(k, get(k)) }
    end

    def default?
      !@default.nil?
    end

    def has_key?(key)
      @table.has_key? key.to_s
    end

    def keys
      @table.keys
    end

    def length
      @table.keys.length
    end

    def serialize(value)
      Marshal.dump(value)
    end

    def parse(value)
      Marshal.load(value)
    end

    def empty!
      reset!
      @writer.truncate!
    end

    def flush!
      @writer.flush!
    end

    def reset!
      @table  = {}
      @writer = Daybreak::Writer.new(@file_name)
    end

    def close!
      @writer.close!
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
      read_all!
    end

    def read_all!
      Reader.new(@file_name).read do |record|
        @table[record.key] = parse record.data
      end
    end
  end
end
