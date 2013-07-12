package org.infinispan.server.memcached

import test.MemcachedTestingUtil._
import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{Test, AfterClass}
import org.infinispan.manager.EmbeddedCacheManager
import java.net.Socket
import collection.mutable.ListBuffer
import java.io.InputStream
import org.testng.Assert._
import java.lang.StringBuilder

/**
 * Base class for single node tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class MemcachedSingleNodeTest extends SingleCacheManagerTest {
   private var memcachedClient: MemcachedClient = _
   private var memcachedServer: MemcachedServer = _
   private val operationTimeout: Int = 60

   override def createCacheManager: EmbeddedCacheManager = {
      cacheManager = createTestCacheManager
      memcachedServer = startMemcachedTextServer(cacheManager)
      memcachedClient = createMemcachedClient(60000, server.getPort)
      cache = cacheManager.getCache[AnyRef, AnyRef](memcachedServer.getConfiguration.cache)
      cacheManager
   }

   protected def createTestCacheManager: EmbeddedCacheManager = TestCacheManagerFactory.createCacheManager(false)

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass() {
      super.destroyAfterClass()
      log.debug("Test finished, close memcached server")
      shutdownClient()
      killMemcachedServer(memcachedServer)
   }

   protected def client: MemcachedClient = memcachedClient

   protected def timeout: Int = operationTimeout

   protected def server: MemcachedServer = memcachedServer

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def shutdownClient() {
      memcachedClient.shutdown()
   }

   protected def send(req: String): String = sendMulti(req, 1, wait = true).head

   protected def sendNoWait(req: String) = sendMulti(req, 1, wait = false)

   protected def sendMulti(req: String, expectedResponses: Int, wait: Boolean): List[String] = {
      val socket = new Socket(server.getHost, server.getPort)
      try {
         socket.getOutputStream.write(req.getBytes)
         if (wait) {
            val buffer = new ListBuffer[String]
            for (i <- 0 until expectedResponses)
               buffer += readLine(socket.getInputStream, new StringBuilder)
            buffer.toList
         } else {
            List()
         }
      }
      finally {
         socket.close()
      }
   }

   protected def readLine(is: InputStream, sb: StringBuilder): String = {
      var next = is.read
      if (next == 13) { // CR
         next = is.read
         if (next == 10) { // LF
            sb.toString.trim
         } else {
            sb.append(next.asInstanceOf[Char])
            readLine(is, sb)
         }
      } else if (next == 10) { //LF
         sb.toString.trim
      } else {
         sb.append(next.asInstanceOf[Char])
         readLine(is, sb)
      }
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def assertClientError(resp: String) {
      assertExpectedResponse(resp, "CLIENT_ERROR", strictComparison = false)
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def assertError(resp: String) {
      assertExpectedResponse(resp, "ERROR", strictComparison = true)
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def assertStored(resp: String) {
      assertExpectedResponse(resp, "STORED", strictComparison = true)
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def assertExpectedResponse(resp: String, expectedResp: String, strictComparison: Boolean) {
      if (strictComparison)
         assertEquals(resp, expectedResp, "Instead response is: " + resp)
      else
         assertTrue(resp.contains(expectedResp), "Instead response is: " + resp)
   }

}