package org.infinispan.server.memcached

import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import java.lang.reflect.Method
import org.testng.Assert._
import java.util.concurrent.TimeUnit
import org.infinispan.Version
import org.infinispan.test.TestingUtil._
import test.MemcachedTestingUtil._
import org.infinispan.manager.EmbeddedCacheManager
import net.spy.memcached.MemcachedClient
import annotation.tailrec
import org.infinispan.server.core.ConnectionStatsTest._

/**
 * Tests stats command for Infinispan Memcached server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedStatsTest")
class MemcachedStatsTest extends MemcachedSingleNodeTest {

   private val jmxDomain = classOf[MemcachedStatsTest].getSimpleName

   override def createTestCacheManager: EmbeddedCacheManager =
      TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)

   def testUnsupportedStats(m: Method) {
      val stats = getStats(-1, -1)
      assertEquals(stats._1.get("pid"), "0")
      assertEquals(stats._1.get("pointer_size"), "0")
      assertEquals(stats._1.get("rusage_user"), "0")
      assertEquals(stats._1.get("rusage_system"), "0")
      assertEquals(stats._1.get("bytes"), "0")
      assertEquals(stats._1.get("connection_structures"), "0")
      assertEquals(stats._1.get("auth_cmds"), "0")
      assertEquals(stats._1.get("auth_errors"), "0")
      assertEquals(stats._1.get("limit_maxbytes"), "0")
      assertEquals(stats._1.get("conn_yields"), "0")
      assertEquals(stats._1.get("reclaimed"), "0")
   }

   def testUncomparableStats(m: Method) {
      sleepThread(TimeUnit.SECONDS.toMillis(1))
      val stats = getStats(-1, -1)
      assertNotSame(stats._1.get("uptime"), "0")
      assertNotSame(stats._1.get("time"), "0")
      assertNotSame(stats._1.get("uptime"), stats._1.get("time"))
   }

   def testStaticStats(m: Method) {
       val stats = getStats(-1, -1)
       assertEquals(stats._1.get("version"), Version.getVersion)
    }

   def testTodoStats() {
      val stats = getStats(-1, -1)
      assertEquals(stats._1.get("curr_connections"), "0")
      assertEquals(stats._1.get("total_connections"), "0")
      assertEquals(stats._1.get("threads"), "0")
   }

   def testStats(m: Method) {
      var stats = getStats(-1, -1)
      assertEquals(stats._1.get("cmd_set"), "0")
      assertEquals(stats._1.get("cmd_get"), "0")
      assertEquals(stats._1.get("get_hits"), "0")
      assertEquals(stats._1.get("get_misses"), "0")
      assertEquals(stats._1.get("delete_hits"), "0")
      assertEquals(stats._1.get("delete_misses"), "0")
      assertEquals(stats._1.get("curr_items"), "0")
      assertEquals(stats._1.get("total_items"), "0")
      assertEquals(stats._1.get("incr_misses"), "0")
      assertEquals(stats._1.get("incr_hits"), "0")
      assertEquals(stats._1.get("decr_misses"), "0")
      assertEquals(stats._1.get("decr_hits"), "0")
      assertEquals(stats._1.get("cas_misses"), "0")
      assertEquals(stats._1.get("cas_hits"), "0")
      assertEquals(stats._1.get("cas_badval"), "0")

      var f = client.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
      f = client.set(k(m, "k1-"), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m, "k1-")), v(m, "v1-"))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("cmd_set"), "2")
      assertEquals(stats._1.get("cmd_get"), "2")
      assertEquals(stats._1.get("get_hits"), "2")
      assertEquals(stats._1.get("get_misses"), "0")
      assertEquals(stats._1.get("delete_hits"), "0")
      assertEquals(stats._1.get("delete_misses"), "0")
      assertEquals(stats._1.get("curr_items"), "2")
      assertEquals(stats._1.get("total_items"), "2")

      f = client.delete(k(m, "k1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("curr_items"), "1")
      assertEquals(stats._1.get("total_items"), "2")
      assertEquals(stats._1.get("delete_hits"), "1")
      assertEquals(stats._1.get("delete_misses"), "0")

      assertNull(client.get(k(m, "k99-")))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("get_hits"), "2")
      assertEquals(stats._1.get("get_misses"), "1")

      f = client.delete(k(m, "k99-"))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("delete_hits"), "1")
      assertEquals(stats._1.get("delete_misses"), "1")

      val future = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 1000).asInstanceOf[Int]
      f = client.set(k(m, "k3-"), future, v(m, "v3-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m, "k3-")))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("curr_items"), "1")
      assertEquals(stats._1.get("total_items"), "3")

      client.incr(k(m, "k4-"), 1)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("incr_misses"), "1")
      assertEquals(stats._1.get("incr_hits"), "0")

      f = client.set(k(m, "k4-"), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      client.incr(k(m, "k4-"), 1)
      client.incr(k(m, "k4-"), 2)
      client.incr(k(m, "k4-"), 4)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("incr_misses"), "1")
      assertEquals(stats._1.get("incr_hits"), "3")

      client.decr(k(m, "k5-"), 1)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("decr_misses"), "1")
      assertEquals(stats._1.get("decr_hits"), "0")

      f = client.set(k(m, "k5-"), 0, "8")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      client.decr(k(m, "k5-"), 1)
      client.decr(k(m, "k5-"), 2)
      client.decr(k(m, "k5-"), 4)
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("decr_misses"), "1")
      assertEquals(stats._1.get("decr_hits"), "3")

      client.cas(k(m, "k6-"), 1234, v(m, "v6-"))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("cas_misses"), "1")
      assertEquals(stats._1.get("cas_hits"), "0")
      assertEquals(stats._1.get("cas_badval"), "0")

      f = client.set(k(m, "k6-"), 0, v(m, "v6-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val value = client.gets(k(m, "k6-"))
      val old: Long = value.getCas
      client.cas(k(m, "k6-"), value.getCas, v(m, "v66-"))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("cas_misses"), "1")
      assertEquals(stats._1.get("cas_hits"), "1")
      assertEquals(stats._1.get("cas_badval"), "0")
      client.cas(k(m, "k6-"), old, v(m, "v66-"))
      stats = getStats(stats._2, stats._3)
      assertEquals(stats._1.get("cas_misses"), "1")
      assertEquals(stats._1.get("cas_hits"), "1")
      assertEquals(stats._1.get("cas_badval"), "1")
   }

   @tailrec
   private def createMultipleClients(clients: List[MemcachedClient], number: Int,
           from: Int) : List[MemcachedClient] = {
      if (from >= number) clients
      else {
         val newClient = createMemcachedClient(60000, server.getPort)
         val value = newClient.get("a")
         // 'Use' the value
         if (value != null && value.hashCode() % 1000 == 0)
            print(value.hashCode())

         createMultipleClients(newClient :: clients, number, from + 1)
      }
   }

   def testStatsSpecificToMemcachedViaJmx() {
      // Send any command
      getStats(-1, -1)

      testSingleLocalConnection(jmxDomain, "Memcached")
      var clients = List[MemcachedClient]()
      try {
         clients = createMultipleClients(clients, 10, 0)
         testMultipleLocalConnections(jmxDomain, "Memcached", clients.size + 1)
      } finally {
         clients.foreach { client =>
            try {
               client.shutdown(20, TimeUnit.SECONDS)
            } catch {
               case t: Throwable => // Ignore it...
            }
         }
      }
   }

   def testStatsWithArgs() {
      var resp = send("stats\r\n")
      assertExpectedResponse(resp, "STAT", strictComparison = false)
      resp = send("stats \r\n")
      assertExpectedResponse(resp, "STAT", strictComparison = false)
      resp = send("stats boo\r\n")
      assertClientError(resp)
      resp = send("stats boo boo2 boo3\r\n")
      assertClientError(resp)
   }

   private def getStats(currentBytesRead: Int, currentBytesWritten: Int) = {
      val globalStats = client.getStats
      assertEquals(globalStats.size(), 1)
      val stats = globalStats.values.iterator.next
      val bytesRead = assertHigherBytes(currentBytesRead, stats.get("bytes_read"))
      val bytesWritten = assertHigherBytes(currentBytesRead, stats.get("bytes_written"))
      (stats, bytesRead, bytesWritten)
   }

   private def assertHigherBytes(currentBytesRead: Int, bytesStr: String): Int = {
      val bytesRead = bytesStr.toInt
      assertTrue(bytesRead > currentBytesRead)
      bytesRead
   }
}