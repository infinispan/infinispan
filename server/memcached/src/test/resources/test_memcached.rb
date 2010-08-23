# encoding: utf-8
require 'rubygems'
require 'logger'
require 'stringio'
require 'test/unit'
$TESTING = true
require 'memcache'

class Test::Unit::TestCase
  def requirement(bool, msg)
    if bool
      yield
    else
      puts msg
      assert true
    end
  end

  def memcached_running?
    TCPSocket.new('localhost', 11211) rescue false
  end
end

class TestMemCache < Test::Unit::TestCase

  def setup
    @cache = MemCache.new 'localhost:1', :namespace => 'my_namespace'
  end

  def test_set_get
    requirement(memcached_running?, 'A real memcached server must be running for performance testing') do
      cache = MemCache.new(['localhost:11211',"127.0.0.1:11211"])
      cache.flush_all
      cache.set('galder', 'b')
      assert_equal 'b', cache.get('galder')
      cache.set('views/semantic/1', 'a')
      assert_equal 'a', cache.get('views/semantic/1')
    end
  end
end
