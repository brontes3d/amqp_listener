RAILS_ENV = "development"
RAILS_ROOT = File.join(File.dirname(__FILE__), "..", "..", "..", "..")
require File.join(File.dirname(__FILE__), "..", "init")

AmqpListener.send(ARGV[0], ARGV[1])