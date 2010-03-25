package org.infinispan.server.hotrod

import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.annotations.{AfterClass, Test}
import java.lang.reflect.Method
import test.{Client, Utils}
import org.testng.Assert._
import java.util.Arrays
import org.jboss.netty.channel.Channel
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import org.infinispan.{AdvancedCache}
import org.infinispan.test.{SingleCacheManagerTest}
import org.infinispan.server.core.CacheValue
import org.infinispan.server.hotrod.OperationStatus._

/**
 * TODO: Document
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
class HotRodFunctionalTest extends SingleCacheManagerTest with Utils with Client {
   private val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var ch: Channel = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createLocalCacheManager(true)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      server = startHotRodServer(cacheManager)
      ch = connect("127.0.0.1", server.getPort)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close client and Hot Rod server", null)
      ch.disconnect
      server.stop
   }

   def testUnknownCommand(m: Method) {
      val status = put(ch, 0xA0, 0x77, cacheName, k(m) , 0, 0, v(m), 0, 0)
      assertTrue(status == UnknownOperation,
         "Status should have been 'UnknownOperation' but instead was: " + status)
   }

   def testUnknownMagic(m: Method) {
      doPut(m) // Do a put to make sure decoder gets back to reading properly
      val status = put(ch, 0x66, 0x01, cacheName, k(m) , 0, 0, v(m), 0, 0)
      assertTrue(status == InvalidMagicOrMsgId,
         "Status should have been 'InvalidMagicOrMsgId' but instead was: " + status)
   }

   // todo: test other error conditions such as invalid version...etc
   // todo: add test for force return value operation

   def testPutBasic(m: Method) {
      doPut(m)
   }

   private def doPut(m: Method) {
      doPutWithLifespanMaxIdle(m, 0, 0)
   }

   private def doPutWithLifespanMaxIdle(m: Method, lifespan: Int, maxIdle: Int) {
      val status = put(ch, cacheName, k(m) , lifespan, maxIdle, v(m))
      assertStatus(status, Success)
   }

   def testPutOnDefaultCache(m: Method) {
      val status = put(ch, DefaultCacheManager.DEFAULT_CACHE_NAME, k(m) , 0, 0, v(m))
      assertStatus(status, Success)
      val cache = cacheManager.getCache[CacheKey, CacheValue]
      val value = cache.get(new CacheKey(k(m)))
      assertTrue(Arrays.equals(value.data, v(m)));
   }

   def testPutWithLifespan(m: Method) {
      doPutWithLifespanMaxIdle(m, 1, 0)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutWithMaxIdle(m: Method) {
      doPutWithLifespanMaxIdle(m, 0, 1)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testGetBasic(m: Method) {
      doPut(m)
      val (getSt, actual) = doGet(m)
      assertSuccess(getSt, v(m), actual)
   }

   def testGetDoesNotExist(m: Method) {
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutIfAbsentNotExist(m: Method) {
      val status = putIfAbsent(ch, cacheName, k(m) , 0, 0, v(m))
      assertStatus(status, Success)
   }

   def testPutIfAbsentExist(m: Method) {
      doPut(m)
      val status = putIfAbsent(ch, cacheName, k(m) , 0, 0, v(m, "v2-"))
      assertStatus(status, OperationNotExecuted)
   }

   def testPutIfAbsentWithLifespan(m: Method) {
      val status = putIfAbsent(ch, cacheName, k(m) , 1, 0, v(m))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testPutIfAbsentWithMaxIdle(m: Method) {
      val status = putIfAbsent(ch, cacheName, k(m) , 0, 1, v(m))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testReplaceBasic(m: Method) {
      doPut(m)
      val status = replace(ch, cacheName, k(m), 0, 0, v(m, "v1-"))
      assertStatus(status, Success)
      val (getSt, actual) = doGet(m)
      assertSuccess(getSt, v(m, "v1-"), actual)
   }

   def testNotReplaceIfNotPresent(m: Method) {
      val status = replace(ch, cacheName, k(m), 0, 0, v(m))
      assertStatus(status, OperationNotExecuted)
   }

   def testReplaceWithLifespan(m: Method) {
      doPut(m)
      val status = replace(ch, cacheName, k(m), 1, 0, v(m, "v1-"))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testReplaceWithMaxIdle(m: Method) {
      doPut(m)
      val status = replace(ch, cacheName, k(m), 0, 1, v(m, "v1-"))
      assertStatus(status, Success)
      Thread.sleep(1100)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testGetWithVersionBasic(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
   }

   def testGetWithVersionDoesNotExist(m: Method) {
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertKeyDoesNotExist(getSt, actual)
      assertTrue(version == 0)
   }

   def testReplaceIfUnmodifiedBasic(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = replaceIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
   }

   def testReplaceIfUnmodifiedNotFound(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = replaceIfUnmodified(ch, cacheName, k(m, "k1-"), 0, 0, v(m, "v1-"), version)
      assertStatus(status, KeyDoesNotExist)
   }

   def testReplaceIfUnmodifiedNotExecuted(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      var status = replaceIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2, version2) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt2, v(m, "v1-"), actual2)
      assertTrue(version2 != 0)
      assertTrue(version != version2)
      status = replaceIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v2-"), version)
      assertStatus(status, OperationNotExecuted)
      status = replaceIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v2-"), version2)
      assertStatus(status, Success)
   }

   def testRemoveBasic(m: Method) {
      doPut(m)
      val status = remove(ch, cacheName, k(m), 0)
      assertStatus(status, Success)
      val (getSt, actual) = doGet(m)
      assertKeyDoesNotExist(getSt, actual)
   }

   def testRemoveDoesNotExist(m: Method) {
      val status = remove(ch, cacheName, k(m), 0)
      assertStatus(status, KeyDoesNotExist)
   }

   def testRemoveIfUnmodifiedBasic(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = removeIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2) = doGet(m)
      assertKeyDoesNotExist(getSt2, actual2)
   }

   def testRemoveIfUnmodifiedNotFound(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      val status = removeIfUnmodified(ch, cacheName, k(m, "k1-"), 0, 0, v(m, "v1-"), version)
      assertStatus(status, KeyDoesNotExist)
      val (getSt2, actual2) = doGet(m)
      assertSuccess(getSt2, v(m), actual2)
   }

   def testRemoveIfUnmodifiedNotExecuted(m: Method) {
      doPut(m)
      val (getSt, actual, version) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt, v(m), actual)
      assertTrue(version != 0)
      var status = replaceIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v1-"), version)
      assertStatus(status, Success)
      val (getSt2, actual2, version2) = getWithVersion(ch, cacheName, k(m), 0)
      assertSuccess(getSt2, v(m, "v1-"), actual2)
      assertTrue(version2 != 0)
      assertTrue(version != version2)
      status = removeIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v2-"), version)
      assertStatus(status, OperationNotExecuted)
      status = removeIfUnmodified(ch, cacheName, k(m), 0, 0, v(m, "v2-"), version2)
      assertStatus(status, Success)
   }

   def testContainsKeyBasic(m: Method) {
      doPut(m)
      val status = containsKey(ch, cacheName, k(m), 0)
      assertStatus(status, Success)
   }

   def testContainsKeyDoesNotExist(m: Method) {
      val status = containsKey(ch, cacheName, k(m), 0)
      assertStatus(status, KeyDoesNotExist)
   }

   def testClear(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         var status = put(ch, cacheName, key , 0, 0, value)
         assertStatus(status, Success)
         status = containsKey(ch, cacheName, key, 0)
         assertStatus(status, Success)
      }

      val status = clear(ch, cacheName)
      assertStatus(status, Success)

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-")
         val status = containsKey(ch, cacheName, key, 0)
         assertStatus(status, KeyDoesNotExist)
      }
   }

   def testStatsDisabled(m: Method) {
      val s = stats(ch, cacheName)
      assertEquals(s.get("timeSinceStart").get, "-1")
      assertEquals(s.get("currentNumberOfEntries").get, "-1")
      assertEquals(s.get("totalNumberOfEntries").get, "-1")
      assertEquals(s.get("stores").get, "-1")
      assertEquals(s.get("retrievals").get, "-1")
      assertEquals(s.get("hits").get, "-1")
      assertEquals(s.get("misses").get, "-1")
      assertEquals(s.get("removeHits").get, "-1")
      assertEquals(s.get("removeMisses").get, "-1")
      assertEquals(s.get("evictions").get, "-1")
   }

   def testPing(m: Method) {
      val status = ping(ch, cacheName)
      assertStatus(status, Success)
   }

   private def doGet(m: Method): (OperationStatus, Array[Byte]) = {
      doGet(m, 0)
   }

   private def doGet(m: Method, flags: Int): (OperationStatus, Array[Byte]) = {
      get(ch, cacheName, k(m), flags)
   }

}