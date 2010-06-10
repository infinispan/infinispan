package org.infinispan.server.hotrod

import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import java.util.Arrays
import org.infinispan.manager.DefaultCacheManager
import org.infinispan.server.core.CacheValue
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test._
import org.infinispan.util.ByteArrayKey

/**
 * Hot Rod server functional test.
 *
 * Note: It appears that optional parameters in annotations result in compiler errors.
 * This has been solved in Scala 2.8.0.Beta1, so use that compiler,
 * otherwise this class won't compile.
 * https://lampsvn.epfl.ch/trac/scala/ticket/1810
 *
 * Keep an eye on that for @Test and @AfterClass annotations
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodFunctionalTest")
class HotRodFunctionalTest extends HotRodSingleNodeTest {

   import HotRodServer.TopologyCacheName

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
      val status = client.execute(0xA0, 0x01, DefaultCacheManager.DEFAULT_CACHE_NAME, k(m), 0, 0, v(m), 0, 1, 0).status
      assertStatus(status, Success)
      val cache = cacheManager.getCache[ByteArrayKey, CacheValue]
      val value = cache.get(new ByteArrayKey(k(m)))
      assertTrue(Arrays.equals(value.data, v(m)));
   }

   def testPutOnUndefinedCache(m: Method) {
      var resp = client.execute(0xA0, 0x01, "boomooo", k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("CacheNotFoundException"))
      assertEquals(resp.status, ServerError, "Status should have been 'ServerError' but instead was: " + resp.status)
      client.assertPut(m)
   }

   def testPutOnTopologyCache(m: Method) {
      val resp = client.execute(0xA0, 0x01, TopologyCacheName, k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("Remote requests are not allowed to topology cache."))
      assertEquals(resp.status, ServerError, "Status should have been 'ServerError' but instead was: " + resp.status)
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
      assertStatus(resp.status, Success)
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
      val status = client.putIfAbsent(k(m) , 0, 0, v(m)).status
      assertStatus(status, Success)
   }

   def testPutIfAbsentExist(m: Method) {
      client.assertPut(m)
      val status = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-")).status
      assertStatus(status, OperationNotExecuted)
   }

   def testPutIfAbsentWithLifespan(m: Method) {
      val status = client.putIfAbsent(k(m) , 1, 0, v(m)).status
      assertStatus(status, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutIfAbsentWithMaxIdle(m: Method) {
      val status = client.putIfAbsent(k(m) , 0, 1, v(m)).status
      assertStatus(status, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testPutIfAbsentWithPreviousValue(m: Method) {
      var resp = client.putIfAbsent(k(m) , 0, 0, v(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, Success)
      assertEquals(resp.previous, None)
      resp = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testReplaceBasic(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 0, 0, v(m, "v1-")).status
      assertStatus(status, Success)
      assertSuccess(client.assertGet(m), v(m, "v1-"))
   }

   def testNotReplaceIfNotPresent(m: Method) {
      val status = client.replace(k(m), 0, 0, v(m)).status
      assertStatus(status, OperationNotExecuted)
   }

   def testReplaceWithLifespan(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 1, 0, v(m, "v1-")).status
      assertStatus(status, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testReplaceWithMaxIdle(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 0, 1, v(m, "v1-")).status
      assertStatus(status, Success)
      Thread.sleep(1100)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testReplaceWithPreviousValue(m: Method) {
      var resp = client.replace(k(m) , 0, 0, v(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, OperationNotExecuted)
      assertEquals(resp.previous, None)
      resp = client.put(k(m) , 0, 0, v(m, "v2-"), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, Success)
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
      assertTrue(resp.version == 0)
   }

   def testReplaceIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, Success)
   }

   def testReplaceIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val status = client.replaceIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, KeyDoesNotExist)
   }

   def testReplaceIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      var resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      var status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, Success)
      val resp2 = client.getWithVersion(k(m), 0)
      assertSuccess(resp2, v(m, "v1-"), 0)
      assertTrue(resp.version != resp2.version)
      status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp.version).status
      assertStatus(status, OperationNotExecuted)
      status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp2.version).status
      assertStatus(status, Success)
   }

   def testReplaceIfUnmodifiedWithPreviousValue(m: Method) {
      var resp = client.replaceIfUnmodified(k(m) , 0, 0, v(m), 999, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      val getResp = client.getWithVersion(k(m), 0)
      assertSuccess(getResp, v(m), 0)
      resp  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
      resp  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v3-"), getResp.version, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, Success)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testRemoveBasic(m: Method) {
      client.assertPut(m)
      val status = client.remove(k(m)).status
      assertStatus(status, Success)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testRemoveDoesNotExist(m: Method) {
      val status = client.remove(k(m)).status
      assertStatus(status, KeyDoesNotExist)
   }

   def testRemoveWithPreviousValue(m: Method) {
      var resp = client.remove(k(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      resp = client.remove(k(m), 1).asInstanceOf[TestResponseWithPrevious]
      assertSuccess(resp, v(m))
   }

   def testRemoveIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      assertTrue(resp.version != 0)
      val status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, Success)
      assertKeyDoesNotExist(client.assertGet(m))
   }

   def testRemoveIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      var resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      val status = client.removeIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, KeyDoesNotExist)
      assertSuccess(client.assertGet(m), v(m))
   }

   def testRemoveIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      val resp = client.getWithVersion(k(m), 0)
      assertSuccess(resp, v(m), 0)
      var status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.version).status
      assertStatus(status, Success)
      val resp2 = client.getWithVersion(k(m), 0)
      assertSuccess(resp2, v(m, "v1-"), 0)
      assertTrue(resp.version != resp2.version)
      status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp.version).status
      assertStatus(status, OperationNotExecuted)
      status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), resp2.version).status
      assertStatus(status, Success)
   }

   def testRemoveIfUmodifiedWithPreviousValue(m: Method) {
      var resp = client.removeIfUnmodified(k(m) , 0, 0, v(m), 999, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, KeyDoesNotExist)
      assertEquals(resp.previous, None)
      client.assertPut(m)
      val getResp = client.getWithVersion(k(m), 0)
      assertSuccess(getResp, v(m), 0)
      resp  = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
      resp = client.removeIfUnmodified(k(m), 0, 0, v(m, "v3-"), getResp.version, 1).asInstanceOf[TestResponseWithPrevious]
      assertStatus(resp.status, Success)
      assertTrue(Arrays.equals(v(m), resp.previous.get))
   }

   def testContainsKeyBasic(m: Method) {
      client.assertPut(m)
      assertStatus(client.containsKey(k(m), 0).status, Success)
   }

   def testContainsKeyDoesNotExist(m: Method) {
      assertStatus(client.containsKey(k(m), 0).status, KeyDoesNotExist)
   }

   def testClear(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         assertStatus(client.put(key, 0, 0, value).status, Success)
         assertStatus(client.containsKey(key, 0).status, Success)
      }

      assertStatus(client.clear.status, Success)

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-")
         assertStatus(client.containsKey(key, 0).status, KeyDoesNotExist)
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
      val status = client.ping.status
      assertStatus(status, Success)
   }

   def testPingWithTopologyAwareClient(m: Method) {
      var resp = client.ping
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(1, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(2, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      resp = client.ping(3, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
   }
   
}