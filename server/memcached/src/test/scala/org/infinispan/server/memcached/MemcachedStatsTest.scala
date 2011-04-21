/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.memcached

import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import java.lang.reflect.Method
import org.testng.Assert._
import java.util.concurrent.TimeUnit
import org.infinispan.Version
import org.infinispan.test.TestingUtil
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Tests stats command for Infinispan Memcached server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedStatsTest")
class MemcachedStatsTest extends MemcachedSingleNodeTest {
   private var jmxDomain = classOf[MemcachedStatsTest].getSimpleName

   override def createTestCacheManager: EmbeddedCacheManager =
      TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)

   def testUnsupportedStats(m: Method) {
      val stats = getStats
      assertEquals(stats.get("pid"), "0")
      assertEquals(stats.get("pointer_size"), "0")
      assertEquals(stats.get("rusage_user"), "0")
      assertEquals(stats.get("rusage_system"), "0")
      assertEquals(stats.get("bytes"), "0")
      assertEquals(stats.get("connection_structures"), "0")
      assertEquals(stats.get("auth_cmds"), "0")
      assertEquals(stats.get("auth_errors"), "0")
      assertEquals(stats.get("limit_maxbytes"), "0")
      assertEquals(stats.get("conn_yields"), "0")
      assertEquals(stats.get("reclaimed"), "0")
   }

   def testUncomparableStats(m: Method) {
      TestingUtil.sleepThread(TimeUnit.SECONDS.toMillis(1))
      val stats = getStats
      assertNotSame(stats.get("uptime"), "0")
      assertNotSame(stats.get("time"), "0")
      assertNotSame(stats.get("uptime"), stats.get("time"))
   }

   def testStaticStats(m: Method) {
       val stats = getStats
       assertEquals(stats.get("version"), Version.VERSION)
    }

   def testTodoStats {
      val stats = getStats
      assertEquals(stats.get("curr_connections"), "0")
      assertEquals(stats.get("total_connections"), "0")
      assertEquals(stats.get("bytes_read"), "0")
      assertEquals(stats.get("bytes_written"), "0")
      assertEquals(stats.get("threads"), "0")
   }


   def testStats(m: Method) {
      var stats = getStats
      assertEquals(stats.get("cmd_set"), "0")
      assertEquals(stats.get("cmd_get"), "0")
      assertEquals(stats.get("get_hits"), "0")
      assertEquals(stats.get("get_misses"), "0")
      assertEquals(stats.get("delete_hits"), "0")
      assertEquals(stats.get("delete_misses"), "0")
      assertEquals(stats.get("curr_items"), "0")
      assertEquals(stats.get("total_items"), "0")
      assertEquals(stats.get("incr_misses"), "0")
      assertEquals(stats.get("incr_hits"), "0")
      assertEquals(stats.get("decr_misses"), "0")
      assertEquals(stats.get("decr_hits"), "0")
      assertEquals(stats.get("cas_misses"), "0")
      assertEquals(stats.get("cas_hits"), "0")
      assertEquals(stats.get("cas_badval"), "0")

      var f = client.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
      f = client.set(k(m, "k1-"), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m, "k1-")), v(m, "v1-"))
      stats = getStats
      assertEquals(stats.get("cmd_set"), "2")
      assertEquals(stats.get("cmd_get"), "2")
      assertEquals(stats.get("get_hits"), "2")
      assertEquals(stats.get("get_misses"), "0")
      assertEquals(stats.get("delete_hits"), "0")
      assertEquals(stats.get("delete_misses"), "0")
      assertEquals(stats.get("curr_items"), "2")
      assertEquals(stats.get("total_items"), "2")

      f = client.delete(k(m, "k1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      stats = getStats
      assertEquals(stats.get("curr_items"), "1")
      assertEquals(stats.get("total_items"), "2")
      assertEquals(stats.get("delete_hits"), "1")
      assertEquals(stats.get("delete_misses"), "0")

      assertNull(client.get(k(m, "k99-")))
      stats = getStats
      assertEquals(stats.get("get_hits"), "2")
      assertEquals(stats.get("get_misses"), "1")

      f = client.delete(k(m, "k99-"))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      stats = getStats
      assertEquals(stats.get("delete_hits"), "1")
      assertEquals(stats.get("delete_misses"), "1")

      val future = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 1000).asInstanceOf[Int]
      f = client.set(k(m, "k3-"), future, v(m, "v3-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      TestingUtil.sleepThread(1100)
      assertNull(client.get(k(m, "k3-")))
      stats = getStats
      assertEquals(stats.get("curr_items"), "1")
      assertEquals(stats.get("total_items"), "3")

      client.incr(k(m, "k4-"), 1)
      stats = getStats
      assertEquals(stats.get("incr_misses"), "1")
      assertEquals(stats.get("incr_hits"), "0")

      f = client.set(k(m, "k4-"), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      client.incr(k(m, "k4-"), 1)
      client.incr(k(m, "k4-"), 2)
      client.incr(k(m, "k4-"), 4)
      stats = getStats
      assertEquals(stats.get("incr_misses"), "1")
      assertEquals(stats.get("incr_hits"), "3")

      client.decr(k(m, "k5-"), 1)
      stats = getStats
      assertEquals(stats.get("decr_misses"), "1")
      assertEquals(stats.get("decr_hits"), "0")

      f = client.set(k(m, "k5-"), 0, "8")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      client.decr(k(m, "k5-"), 1)
      client.decr(k(m, "k5-"), 2)
      client.decr(k(m, "k5-"), 4)
      stats = getStats
      assertEquals(stats.get("decr_misses"), "1")
      assertEquals(stats.get("decr_hits"), "3")

      client.cas(k(m, "k6-"), 1234, v(m, "v6-"))
      stats = getStats
      assertEquals(stats.get("cas_misses"), "1")
      assertEquals(stats.get("cas_hits"), "0")
      assertEquals(stats.get("cas_badval"), "0")

      f = client.set(k(m, "k6-"), 0, v(m, "v6-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      var value = client.gets(k(m, "k6-"))
      var old: Long = value.getCas
      client.cas(k(m, "k6-"), value.getCas, v(m, "v66-"))
      stats = getStats
      assertEquals(stats.get("cas_misses"), "1")
      assertEquals(stats.get("cas_hits"), "1")
      assertEquals(stats.get("cas_badval"), "0")
      client.cas(k(m, "k6-"), old, v(m, "v66-"))
      stats = getStats
      assertEquals(stats.get("cas_misses"), "1")
      assertEquals(stats.get("cas_hits"), "1")
      assertEquals(stats.get("cas_badval"), "1")
   }

   def testStatsWithArgs {
      var resp = send("stats\r\n")
      assertExpectedResponse(resp, "STAT", false)
      resp = send("stats \r\n")
      assertExpectedResponse(resp, "STAT", false)
      resp = send("stats boo\r\n")
      assertClientError(resp)
      resp = send("stats boo boo2 boo3\r\n")
      assertClientError(resp)
   }
   
   private def getStats() = {
      val stats = client.getStats()
      assertEquals(stats.size(), 1)
      stats.values.iterator.next
   }

}