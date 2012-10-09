module Falconer
  module Locking
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
