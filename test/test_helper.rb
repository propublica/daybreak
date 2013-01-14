HERE = File.expand_path(File.dirname(__FILE__))
DB_PATH = File.join HERE, "test.db"

$: << File.join(HERE, '..', 'lib')
require 'daybreak'
