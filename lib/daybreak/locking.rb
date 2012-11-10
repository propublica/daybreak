module Daybreak
  # File locking mixin
  module Locking
    # Lock a file with the type <tt>lock</tt>
    def lock(fd, lock=File::LOCK_SH)
      fd.flock lock
      begin
        yield
      ensure
        fd.flock File::LOCK_UN
      end
    end
  end
end
