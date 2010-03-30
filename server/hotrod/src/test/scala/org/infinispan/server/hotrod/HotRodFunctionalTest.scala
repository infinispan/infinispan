package org.infinispan.server.hotrod

import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.annotations.{AfterClass, Test}
import java.lang.reflect.Method
import test.HotRodClient
import test.HotRodTestingUtil._
import org.testng.Assert._
import java.util.Arrays
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import org.infinispan.{AdvancedCache}
import org.infinispan.test.{SingleCacheManagerTest}
import org.infinispan.server.core.CacheValue
import org.infinispan.server.hotrod.OperationStatus._

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
class HotRodFunctionalTest extends SingleCacheManagerTest {
   private val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var client: HotRodClient = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createLocalCacheManager(true)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      server = startHotRodServer(cacheManager)
      client = new HotRodClient("127.0.0.1", server.getPort, cacheName)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close client and Hot Rod server", null)
      client.stop
      server.stop
   }

   def testUnknownCommand(m: Method) {
      val status = client.execute(0xA0, 0x77, cacheName, k(m) , 0, 0, v(m), 0)
      assertTrue(status == UnknownOperation,
         "Status should have been 'UnknownOperation' but instead was: " + status)
   }

   def testUnknownMagic(m: Method) {
      client.assertPut(m) // Do a put to make sure decoder gets back to reading properly
      val status = client.executeWithBadMagic(0x66, 0x01, cacheName, k(m) , 0, 0, v(m), 0)
      assertTrue(status == InvalidMagicOrMsgId,
         "Status should have been 'InvalidMagicOrMsgId' but instead was: " + status)
   }

   // todo: test other error conditions such as invalid version...etc

   def testPutBasic(m: Method) {
      client.assertPut(m)
   }

   def testPutOnDefaultCache(m: Method) {
      val status = client.execute(0xA0, 0x01, DefaultCacheManager.DEFAULT_CACHE_NAME, k(m), 0, 0, v(m), 0)
      assertStatus(status, Success)
      val cache = cacheManager.getCache[CacheKey, CacheValue]
      val value = cache.get(new CacheKey(k(m)))
      assertTrue(Arrays.equals(value.data, v(m)));
   }

   def testPutWithLifespan(m: Method) {
      client.assertPut(m, 1, 0)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutWithMaxIdle(m: Method) {
      client.assertPut(m, 0, 1)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutWithPreviousValue(m: Method) {
      val (status, previous) = client.put(k(m) , 0, 0, v(m), 1)
      assertSuccess(status, Array(), previous)
      val (status2, previous2) = client.put(k(m) , 0, 0, v(m, "v2-"), 1)
      assertSuccess(status2, v(m), previous2)
   }

   def testGetBasic(m: Method) {
      client.assertPut(m)
      val (getSt, actual) = client.assertGet(m)
      assertSuccess(getSt, v(m), actual)
   }

   def testGetDoesNotExist(m: Method) {
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutIfAbsentNotExist(m: Method) {
      val status = client.putIfAbsent(k(m) , 0, 0, v(m))
      assertStatus(status, Success)
   }

   def testPutIfAbsentExist(m: Method) {
      client.assertPut(m)
      val status = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-"))
      assertStatus(status, OperationNotExecuted)
   }

   def testPutIfAbsentWithLifespan(m: Method) {
      val status = client.putIfAbsent(k(m) , 1, 0, v(m))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutIfAbsentWithMaxIdle(m: Method) {
      val status = client.putIfAbsent(k(m) , 0, 1, v(m))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutIfAbsentWithPreviousValue(m: Method) {
      val (status, previous) = client.putIfAbsent(k(m) , 0, 0, v(m), 1)
      assertSuccess(status, Array(), previous)
      val (status2, previous2) = client.putIfAbsent(k(m) , 0, 0, v(m, "v2-"), 1)
      assertStatus(status2, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), previous2))
   }

   def testReplaceBasic(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 0, 0, v(m, "v1-"))
      assertStatus(status, Success)
      val (getSt, actual) = client.assertGet(m)
      assertSuccess(getSt, v(m, "v1-"), actual)
   }

   def testNotReplaceIfNotPresent(m: Method) {
      val status = client.replace(k(m), 0, 0, v(m))
      assertStatus(status, OperationNotExecuted)
   }

   def testReplaceWithLifespan(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 1, 0, v(m, "v1-"))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testReplaceWithMaxIdle(m: Method) {
      client.assertPut(m)
      val status = client.replace(k(m), 0, 1, v(m, "v1-"))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testReplaceWithPreviousValue(m: Method) {
      val (status, previous) = client.replace(k(m) , 0, 0, v(m), 1)
      assertStatus(status, OperationNotExecuted)
      assertEquals(previous.length, 0)
      val (status2, previous2) = client.put(k(m) , 0, 0, v(m, "v2-"), 1)
      assertSuccess(status2, Array(), previous2)
      val (status3, previous3) = client.replace(k(m) , 0, 0, v(m, "v3-"), 1)
      assertSuccess(status3, v(m, "v2-"), previous3)
   }

   def testGetWithVersionBasic(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
   }

   def testGetWithVersionDoesNotExist(m: Method) {
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertKeyDoesNotExist(getSt, actual)
      assertTrue(version == 0)
   }

   def testReplaceIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
   }

   def testReplaceIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = client.replaceIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), version)
      assertStatus(status, KeyDoesNotExist)
   }

   def testReplaceIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      var status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2, version2) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt2, v(m, "v1-"), actual2)
      assertTrue(version2 != 0)
      assertTrue(version != version2)
      status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), version)
      assertStatus(status, OperationNotExecuted)
      status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), version2)
      assertStatus(status, Success)
   }

   def testReplaceIfUnmodifiedWithPreviousValue(m: Method) {
      val (status, previous) = client.replaceIfUnmodified(k(m) , 0, 0, v(m), 999, 1)
      assertStatus(status, KeyDoesNotExist)
      assertEquals(previous.length, 0)
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val (status2, previous2)  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1)
      assertStatus(status2, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), previous2))
      val (status3, previous3)  = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v3-"), version, 1)
      assertStatus(status3, Success)
      assertTrue(Arrays.equals(v(m), previous3))
   }

   def testRemoveBasic(m: Method) {
      client.assertPut(m)
      val status = client.remove(k(m))
      assertStatus(status, Success)
      val (getSt, actual) = client.assertGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testRemoveDoesNotExist(m: Method) {
      val status = client.remove(k(m))
      assertStatus(status, KeyDoesNotExist)
   }

   def testRemoveWithPreviousValue(m: Method) {
      val (status, previous) = client.remove(k(m), 1)
      assertStatus(status, KeyDoesNotExist)
      assertEquals(previous.length, 0)
      client.assertPut(m)
      val (status2, previous2) = client.remove(k(m), 1)
      assertSuccess(status2, v(m), previous2)
   }

   def testRemoveIfUnmodifiedBasic(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2) = client.assertGet(m)
      assertKeyDoesNotExist(getSt2, actual2)
   }

   def testRemoveIfUnmodifiedNotFound(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = client.removeIfUnmodified(k(m, "k1-"), 0, 0, v(m, "v1-"), version)
      assertStatus(status, KeyDoesNotExist)
      val (getSt2, actual2) = client.assertGet(m)
      assertSuccess(getSt2, v(m), actual2)
   }

   def testRemoveIfUnmodifiedNotExecuted(m: Method) {
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      var status = client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2, version2) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt2, v(m, "v1-"), actual2)
      assertTrue(version2 != 0)
      assertTrue(version != version2)
      status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), version)
      assertStatus(status, OperationNotExecuted)
      status = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), version2)
      assertStatus(status, Success)
   }

   def testRemoveIfUmodifiedWithPreviousValue(m: Method) {
      val (status, previous) = client.removeIfUnmodified(k(m) , 0, 0, v(m), 999, 1)
      assertStatus(status, KeyDoesNotExist)
      assertEquals(previous.length, 0)
      client.assertPut(m)
      val (getSt, actual, version) = client.getWithVersion(k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val (status2, previous2)  = client.removeIfUnmodified(k(m), 0, 0, v(m, "v2-"), 888, 1)
      assertStatus(status2, OperationNotExecuted)
      assertTrue(Arrays.equals(v(m), previous2))
      val (status3, previous3)  = client.removeIfUnmodified(k(m), 0, 0, v(m, "v3-"), version, 1)
      assertStatus(status3, Success)
      assertTrue(Arrays.equals(v(m), previous3))
   }

   def testContainsKeyBasic(m: Method) {
      client.assertPut(m)
      val status = client.containsKey(k(m), 0)
      assertStatus(status, Success)
   }

   def testContainsKeyDoesNotExist(m: Method) {
      val status = client.containsKey(k(m), 0)
      assertStatus(status, KeyDoesNotExist)
   }

   def testClear(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         var status = client.put(key , 0, 0, value)
         assertStatus(status, Success)
         status = client.containsKey(key, 0)
         assertStatus(status, Success)
      }

      val status = client.clear
      assertStatus(status, Success)

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-")
         val status = client.containsKey(key, 0)
         assertStatus(status, KeyDoesNotExist)
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
      val status = client.ping
      assertStatus(status, Success)
   }

}