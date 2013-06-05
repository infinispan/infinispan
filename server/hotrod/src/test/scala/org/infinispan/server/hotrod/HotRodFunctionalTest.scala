/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.server.hotrod

import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import java.util.Arrays
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test._
import org.infinispan.test.TestingUtil.generateRandomString
import org.infinispan.config.Configuration
import java.util.concurrent.TimeUnit
import org.infinispan.server.core.test.Stoppable
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration

/**
 * Hot Rod server functional test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodFunctionalTest")
class HotRodFunctionalTest extends HotRodSingleNodeTest {

   def testUnknownCommand(m: Method) {
      val status = client.execute(0xA0, 0x77, cacheName, k(m) , 0, 0, v(m), 0, 1, 0).status
      assertEquals(status, UnknownOperation,
         "Status should have been 'UnknownOperation' but instead was: " + status)
   }

   def testUnknownMagic(m: Method) {
      client.assertPut(m) // Do a put to make sure decoder gets back to reading properly
      val status = client.executeExpectBadMagic(0x66, 0x01, cacheName, k(m) , 0, 0, v(m), 0).status
      assertEquals(status, InvalidMagicOrMsgId,
         "Status should have been 'InvalidMagicOrMsgId' but instead was: " + status)
   }

   // todo: test other error conditions such as invalid version...etc

   def testPutBasic(m: Method) {
      client.assertPut(m)
   }

   def testPutOnDefaultCache(m: Method) {
      val resp = client.execute(0xA0, 0x01, "", k(m), 0, 0, v(m), 0, 1, 0)
      assertStatus(resp, Success)
      assertHotRodEquals(cacheManager, k(m), v(m))
   }

   def testPutOnUndefinedCache(m: Method) {
      val resp = client.execute(0xA0, 0x01, "boomooo", k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("CacheNotFoundException"))
      assertEquals(resp.status, ParseError, "Status should have been 'ParseError' but instead was: " + resp.status)
      client.assertPut(m)
   }

   def testPutOnTopologyCache(m: Method) {
      val resp = client.execute(0xA0, 0x01, HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX, k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("Remote requests are not allowed to topology cache."))
      assertEquals(resp.status, ParseError, "Status should have been 'ParseError' but instead was: " + resp.status)
      client.assertPut(m)
   }

   def testPutWithLifespan(m: Method) {
      client.assertPut(m, 1, 0)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutWithMaxIdle(m: Method) {
      client.assertPut(m, 0, 1)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutWithPreviousValue(m: Method) {
      var resp = client.put(k(m) , 0, 0, v(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, Success)
      assertEquals(resp.previous, None)
      resp = client.put(k(m) , 0, 0, v(m, "v2-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertSuccess(resp, v(m))
   }

   def testGetBasic(m: Method) {
      client.assertPut(m)
      assertSuccess(client.assertGet(m), v(m))
   }

   def testGetDoesNotExist(m: Method) {
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutIfAbsentNotExist(m: Method) {
      val resp = client.putIfAbsent(k(m) , 0, 0, v(m))
      assertStatus(resp, Success)
   }

   def testPutIfAbsentExist(m: Method) {
      client.assertPut(m)
      val resp = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-"))
      assertStatus(resp, OperationNotExecuted)
   }

   def testPutIfAbsentWithLifespan(m: Method) {
      val resp = client.putIfAbsent(k(m) , 1, 0, v(m))
      assertStatus(resp, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutIfAbsentWithMaxIdle(m: Method) {
      val resp = client.putIfAbsent(k(m) , 0, 1, v(m))
      assertStatus(resp, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutIfAbsentWithPreviousValue(m: Method) {
      var resp = client.putIfAbsent(k(m) , 0, 0, v(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, Success)
      assertEquals(resp.previous, None)
      resp = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testReplaceBasic(m: Method) {
      client.assertPut(m)
      val resp = client.replace(k(m), 0, 0, v(m, "v1-"))
      assertStatus(resp, Success)
      assertSuccess(client.assertGet(m), v(m, "v1-"))
   }

   def testNotReplaceIfNotPresent(m: Method) {
      val resp = client.replace(k(m), 0, 0, v(m))
      assertStatus(resp, OperationNotExecuted)
   }

   def testReplaceWithLifespan(m: Method) {
      client.assertPut(m)
      val resp = client.replace(k(m), 1, 0, v(m, "v1-"))
      assertStatus(resp, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testReplaceWithMaxIdle(m: Method) {
      client.assertPut(m)
      val resp = client.replace(k(m), 0, 1, v(m, "v1-"))
      assertStatus(resp, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testReplaceWithPreviousValue(m: Method) {
      var resp = client.replace(k(m) , 0, 0, v(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, OperationNotExecuted)
      assertEquals(resp.previous, None)
      resp = client.put(k(m) , 0, 0, v(m, "v2-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, Success)
      assertEquals(resp.previous, None)
      resp = client.replace(k(m) , 0, 0, v(m, "v3-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertSuccess(resp, v(m, "v2-"))
   }

   def testGetWithVersionBasic(m: Method) {
      client.assertPut(m)
      assertSuccess(client.getWithVersion(k(m), 0), v(m), 0)
   }

   def testGetWithVersionDoesNotExist(m: Method) {
      val resp = client.getWithVersion(k(m), 0)
      assertKeyDoesNotExist(resp)
      assertTrue(resp.dataVersion == 0)
   }

   def testGetWithMetadata(m: Method) {
      client.assertPut(m)
      assertSuccess(client.assertGet(m), v(m))
      assertSuccess(client.getWithMetadata(k(m), 0), v(m), -1, -1)
      client.remove(k(m))
      client.assertPut(m, 10, 5)
      assertSuccess(client.getWithMetadata(k(m), 0), v(m), 10, 5)
   }

   def testReplaceIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val resp2 = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, Success)
   }

   def testReplaceIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val resp2 = client.replaceIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, KeyDoesNotExist)
   }

   def testReplaceIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val resp2 = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, Success)
      val resp3 = client.getWithVersion(k(m), 0)
      assertSuccess(resp3, v(m, "v1-"), 0)
      assertTrue(resp.dataVersion != resp3.dataVersion)
      val resp4 = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp.dataVersion)
      assertStatus(resp4, OperationNotExecuted)
      val resp5 = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp3.dataVersion)
      assertStatus(resp5, Success)
   }

   def testReplaceIfUnmodifiedWithPreviousValue(m: Method) {
      var resp = client.replaceIfUnmodified(k(m) , 0, 0, v(m), 999, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      val getResp = client.getWithVersion(k(m), 0)
      assertSuccess(getResp, v(m), 0)
      resp  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
      resp  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v3-"), getResp.dataVersion, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, Success)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testRemoveIfUnmodifiedWithExpiry(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      assertTrue(resp.dataVersion != 0)

      val lifespanSecs = 2
      val lifespan = TimeUnit.SECONDS.toMillis(lifespanSecs)
      val startTime = System.currentTimeMillis
      val resp2 = client.replaceIfUnmodified(k(m), lifespanSecs, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, Success)

      while (System.currentTimeMillis < startTime + lifespan) {
         val getResponse = client.assertGet(m)
         // The entry could have expired before our request got to the server
         // Scala doesn't support break, so we need to test the current time twice
         if (System.currentTimeMillis < startTime + lifespan) {
            assertSuccess(getResponse, v(m, "v1-"))
            Thread.sleep(100)
         }
      }

      waitNotFound(startTime, lifespan, m)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   private def waitNotFound(startTime: Long, lifespan: Long, m: Method) {
      if (System.currentTimeMillis < startTime + lifespan + 20000) {
         if (Success == client.assertGet(m)) {
            Thread.sleep(50)
            waitNotFound(startTime, lifespan, m)
         }
      }
   }

   def testRemoveBasic(m: Method) {
      client.assertPut(m)
      val resp = client.remove(k(m))
      assertStatus(resp, Success)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testRemoveDoesNotExist(m: Method) {
      assertStatus(client.remove(k(m)), KeyDoesNotExist)
   }

   def testRemoveWithPreviousValue(m: Method) {
      var resp = client.remove(k(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      resp = client.remove(k(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertSuccess(resp, v(m))
   }

   def testRemoveIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      assertTrue(resp.dataVersion != 0)
      val resp2 = client.removeIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, Success)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testRemoveIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val resp2 = client.removeIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, KeyDoesNotExist)
      assertSuccess(client.assertGet(m), v(m))
   }

   def testRemoveIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val resp2 = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
      assertStatus(resp2, Success)
      val resp3 = client.getWithVersion(k(m), 0)
      assertSuccess(resp3, v(m, "v1-"), 0)
      assertTrue(resp.dataVersion != resp3.dataVersion)
      val resp4 = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp.dataVersion)
      assertStatus(resp4, OperationNotExecuted)
      val resp5 = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp3.dataVersion)
      assertStatus(resp5, Success)
   }

   def testRemoveIfUmodifiedWithPreviousValue(m: Method) {
      var resp = client.removeIfUnmodified(k(m) , 0, 0, v(m), 999, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      val getResp = client.getWithVersion(k(m), 0)
      assertSuccess(getResp, v(m), 0)
      resp  = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
      resp = client.removeIfUnmodified(k(m), 0, 0, v(m, "v3-"), getResp.dataVersion, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp, Success)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testContainsKeyBasic(m: Method) {
      client.assertPut(m)
      assertStatus(client.containsKey(k(m), 0), Success)
   }

   def testContainsKeyDoesNotExist(m: Method) {
      assertStatus(client.containsKey(k(m), 0), KeyDoesNotExist)
   }

   def testClear(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         assertStatus(client.put(key, 0, 0, value), Success)
         assertStatus(client.containsKey(key, 0), Success)
      }

      assertStatus(client.clear, Success)

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-")
         assertStatus(client.containsKey(key, 0), KeyDoesNotExist)
      }
   }

   def testStatsDisabled(m: Method) {
      val s = client.stats
      assertEquals(s.get("timeSinceStart").get, "-1")
      assertEquals(s.get("currentNumberOfEntries").get, "-1")
      assertEquals(s.get("totalNumberOfEntries").get, "-1")
      assertEquals(s.get("stores").get, "-1")
      assertEquals(s.get("retrievals").get, "-1")
      assertEquals(s.get("hits").get, "-1")
      assertEquals(s.get("misses").get, "-1")
      assertEquals(s.get("removeHits").get, "-1")
      assertEquals(s.get("removeMisses").get, "-1")
   }

   def testPing(m: Method) {
      assertStatus(client.ping, Success)
   }

   def testPingWithTopologyAwareClient(m: Method) {
      var resp = client.ping
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(1, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(2, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(3, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
   }

   def testBulkGet(m: Method) {
      var size = 100
      for (i <- 0 until size) {
         val resp = client.put(k(m, i + "k-") , 0, 0, v(m, i + "v-"))
         assertStatus(resp, Success)
      }
      var resp = client.bulkGet
      assertStatus(resp, Success)
      var bulkData = resp.bulkData
      assertEquals(size, bulkData.size)
      for (i <- 0 until size) {
         val key = k(m, i + "k-")
         val filtered = bulkData.filter {
            case (k, v) => java.util.Arrays.equals(k, key)
         }
         assertEquals(1, filtered.size)
      }

      size = 50
      resp = client.bulkGet(size)
      assertStatus(resp, Success)
      bulkData = resp.bulkData
      assertEquals(size, bulkData.size)
      for (i <- 0 until size) {
         val key = k(m, i + "k-")
         val filtered = bulkData.filter { case (k, v) =>
            java.util.Arrays.equals(k, key)
         }

         if (!filtered.isEmpty) {
            assertTrue(java.util.Arrays.equals(filtered.head._2, v(m, i + "v-")))
         }
      }
   }

   def testBulkGetKeys(m: Method) {
      var size = 100
      for (i <- 0 until size) {
         val resp = client.put(k(m, i + "k-") , 0, 0, v(m, i + "v-"))
         assertStatus(resp, Success)
      }
      var resp = client.bulkGetKeys
      assertStatus(resp, Success)
      var bulkData = resp.bulkData
      assertEquals(size, bulkData.size)
      for (i <- 0 until size) {
         val filtered = bulkData.filter(java.util.Arrays.equals(_, k(m, i + "k-")))
         assertEquals(1, filtered.size)
      }

      resp = client.bulkGetKeys(1)
      assertStatus(resp, Success)
      bulkData = resp.bulkData
      assertEquals(size, bulkData.size)
      for (i <- 0 until size) {
         val filtered = bulkData.filter(java.util.Arrays.equals(_, k(m, i + "k-")))
         assertEquals(1, filtered.size)
      }

      resp = client.bulkGetKeys(2)
      assertStatus(resp, Success)
      bulkData = resp.bulkData
      assertEquals(size, bulkData.size)
      for (i <- 0 until size) {
         val filtered = bulkData.filter(java.util.Arrays.equals(_, k(m, i + "k-")))
         assertEquals(1, filtered.size)
      }
   }

   def testPutBigSizeKey(m: Method) {
      val key = generateRandomString(1024 * 1024).getBytes
      assertStatus(client.put(key, 0, 0, v(m)), Success)
   }

   def testPutBigSizeValue(m: Method) {
      val value = generateRandomString(1024 * 1024).getBytes
      assertStatus(client.put(k(m), 0, 0, value), Success)
   }

   def testStoreAsBinaryOverrideOnNamedCache(m: Method) {
      Stoppable.useCacheManager(createTestCacheManager) { cm =>
         Stoppable.useServer(startHotRodServer(cm, server.getPort + 33)) { server =>
            val cacheName = "cache-" + m.getName
            val namedCfg = new Configuration().fluent.storeAsBinary.build
            assertTrue(namedCfg.isStoreAsBinary)
            cm.defineConfiguration(cacheName, namedCfg)
            assertFalse(cm.getCache(cacheName).getConfiguration.isStoreAsBinary)
         }
      }
   }

}