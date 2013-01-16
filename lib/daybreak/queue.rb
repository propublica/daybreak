if !defined?(RUBY_ENGINE) || RUBY_ENGINE == 'ruby'
  require 'daybreak/queue/mri'
else
  require 'daybreak/queue/threaded'
end
