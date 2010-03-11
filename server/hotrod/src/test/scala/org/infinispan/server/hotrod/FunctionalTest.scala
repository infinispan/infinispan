package org.infinispan.server.hotrod

import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.annotations.{AfterClass, Test}
import java.lang.reflect.Method
import test.{Client, Utils}
import org.testng.Assert._
import org.infinispan.server.hotrod.Status._
import java.util.Arrays
import org.jboss.netty.channel.Channel
import org.infinispan.manager.{DefaultCacheManager, CacheManager}
import org.infinispan.context.Flag
import org.infinispan.{AdvancedCache, Cache => InfinispanCache}
import org.infinispan.test.{SingleCacheManagerTest}

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
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
class FunctionalTest extends SingleCacheManagerTest with Utils with Client {
   private val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var ch: Channel = _
   private var advancedCache: AdvancedCache[Key, Value] = _
//   private var tm: TransactionManager = _

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createLocalCacheManager(true)
      advancedCache = cacheManager.getCache[Key, Value](cacheName).getAdvancedCache
//      tm = TestingUtil.getTransactionManager(advancedCache)
      server = createHotRodServer(cacheManager)
      server.start
      ch = connect("127.0.0.1", server.port)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close Hot Rod server", null)
      server.stop
   }

   def testUnknownCommand(m: Method) {
      val status = put(ch, 0xA0, 0x77, cacheName, k(m) , 0, 0, v(m), null)
      assertTrue(status == UnknownCommand,
         "Status should have been 'UnknownCommand' but instead was: " + status)
   }

   def testUnknownMagic(m: Method) {
      doPut(m) // Do a put to make sure decoder gets back to reading properly
      val status = put(ch, 0x66, 0x01, cacheName, k(m) , 0, 0, v(m), null)
      assertTrue(status == InvalidMagicOrMsgId,
         "Status should have been 'InvalidMagicOrMsgId' but instead was: " + status)
   }

   // todo: test other error conditions such as invalid version...etc

   def testPutBasic(m: Method) {
      doPut(m)
   }

   def testPutOnDefaultCache(m: Method) {
      val status = put(ch, DefaultCacheManager.DEFAULT_CACHE_NAME, k(m) , 0, 0, v(m))
      assertStatus(status, Success)
      val cache: InfinispanCache[Key, Value] = cacheManager.getCache[Key, Value]
      assertTrue(Arrays.equals(cache.get(new Key(k(m))).v, v(m)));
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

   

// Invalid test since starting transactions does not make sense
// TODO: discuss flags with list
// def testGetWithWriteLock(m: Method) {
//      doPut(m)
//      assertNotLocked(advancedCache, new Key(k(m)))
//      tm.begin
//      doGet(m, Set(Flag.FORCE_WRITE_LOCK))
//      assertLocked(advancedCache, new Key(k(m)))
//      tm.commit
//      assertNotLocked(advancedCache, new Key(k(m)))
//   }

//   private def transactional(op: Unit => Unit) {
//      tm.begin
//      try {
//         op()
//      } catch {
//         case _ => tm.setRollbackOnly
//      } finally {
//         if (tm.getStatus == TransactionStatus.STATUS_ACTIVE) tm.commit
//         else tm.rollback
//      }
//   }

//   private def assertSuccess(status: Status.Status) {
//      assertTrue(status == Success, "Status should have been 'Success' but instead was: " + status)
//   }
//
//   private def assertSuccess(status: Status.Status, expected: Array[Byte], actual: Array[Byte]) {
//      assertSuccess(status)
//      assertTrue(Arrays.equals(expected, actual))
//   }
//
//   private def assertKeyDoesNotExist(status: Status.Status, actual: Array[Byte]) {
//      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
//      assertNull(actual)
//   }

   private def doPut(m: Method) {
      doPutWithLifespanMaxIdle(m, 0, 0)
   }

   private def doPutWithLifespanMaxIdle(m: Method, lifespan: Int, maxIdle: Int) {
      val status = put(ch, cacheName, k(m) , lifespan, maxIdle, v(m))
      assertStatus(status, Success)
   }

   private def doGet(m: Method): (Status.Status, Array[Byte]) = {
      doGet(m, null)
   }

   private def doGet(m: Method, flags: Set[Flag]): (Status.Status, Array[Byte]) = {
      get(ch, cacheName, k(m), flags)
   }

}