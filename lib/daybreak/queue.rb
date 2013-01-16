module Daybreak
  # Thread safe job queue
  # @api private
  if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
    require 'daybreak/queue/queue_mri'
  else
    require 'daybreak/queue/queue_threaded'
  end
end
