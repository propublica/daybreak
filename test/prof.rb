require File.expand_path(File.dirname(__FILE__)) + '/test_helper.rb'

require 'ruby-prof'

result = RubyProf.profile do
  db = Daybreak::DB.new './t.db'
  1000.times {|n| db[n] = n}
  db.flush!
end

File.unlink './t.db'
printer = RubyProf::MultiPrinter.new(result)
FileUtils.mkdir('./profile') unless File.exists? './profile'
printer.print :path => './profile', :profile => 'profile'