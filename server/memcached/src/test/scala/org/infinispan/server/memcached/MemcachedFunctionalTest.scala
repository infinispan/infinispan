package org.infinispan.server.memcached

import java.lang.reflect.Method
import java.util.concurrent.TimeUnit
import org.testng.Assert._
import org.testng.annotations.Test
import net.spy.memcached.CASResponse
import org.infinispan.test.TestingUtil._
import org.infinispan.Version
import java.net.Socket
import collection.mutable.ListBuffer
import java.io.InputStream

/**
 * Tests Memcached protocol functionality against Infinispan Memcached server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedFunctionalTest")
class MemcachedFunctionalTest extends MemcachedSingleNodeTest {

   def testSetBasic(m: Method) {
      val f = client.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
   }

   def testSetWithExpirySeconds(m: Method) {
      val f = client.set(k(m), 1, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m)))
   }

   def testSetWithExpiryUnixTime(m: Method) {
      val future = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 1000).asInstanceOf[Int]
      val f = client.set(k(m), future, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m)))
   }

   def testSetWithExpiryUnixTimeInPast(m: Method) {
      val f = client.set(k(m), 60*60*24*30 + 1, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m)))
   }

   def testGetMultipleKeys(m: Method) {
      val f1 = client.set(k(m, "k1-"), 0, v(m, "v1-"))
      val f2 = client.set(k(m, "k2-"), 0, v(m, "v2-"))
      val f3 = client.set(k(m, "k3-"), 0, v(m, "v3-"))
      assertTrue(f1.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertTrue(f2.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertTrue(f3.get(timeout, TimeUnit.SECONDS).booleanValue)
      val keys = List(k(m, "k1-"), k(m, "k2-"), k(m, "k3-"))
      val ret = client.getBulk(keys: _*)
      assertEquals(ret.get(k(m, "k1-")), v(m, "v1-"))
      assertEquals(ret.get(k(m, "k2-")), v(m, "v2-"))
      assertEquals(ret.get(k(m, "k3-")), v(m, "v3-"))
   }

   def testAddBasic(m: Method) {
      addAndGet(m)
   }

   def testAddWithExpirySeconds(m: Method) {
      var f = client.add(k(m), 1, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m)))
      f = client.add(k(m), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m, "v1-"))
   }

   def testAddWithExpiryUnixTime(m: Method) {
      val future = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 1000).asInstanceOf[Int]
      var f = client.add(k(m), future, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sleepThread(1100)
      assertNull(client.get(k(m)))
      f = client.add(k(m), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m, "v1-"))
   }

   def testNotAddIfPresent(m: Method) {
      addAndGet(m)
      val f = client.add(k(m), 0, v(m, "v1-"))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
   }

   def testReplaceBasic(m: Method) {
      addAndGet(m)
      val f = client.replace(k(m), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m, "v1-"))
   }

   def testNotReplaceIfNotPresent(m: Method) {
      val f = client.replace(k(m), 0, v(m))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertNull(client.get(k(m)))
   }

   def testReplaceWithExpirySeconds(m: Method) {
      addAndGet(m)
      val f = client.replace(k(m), 1, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m, "v1-"))
      sleepThread(1100)
      assertNull(client.get(k(m)))
   }

   def testReplaceWithExpiryUnixTime(m: Method) {
      addAndGet(m)
      val future: Int = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 1000).asInstanceOf[Int]
      val f = client.replace(k(m), future, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m, "v1-"))
      sleepThread(1100)
      assertNull(client.get(k(m)))
   }

   def testAppendBasic(m: Method) {
      addAndGet(m)
      val f = client.append(0, k(m), v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val expected = v(m) + v(m, "v1-")
      assertEquals(client.get(k(m)), expected)
   }

   def testAppendNotFound(m: Method) {
      addAndGet(m)
      val f = client.append(0, k(m, "k2-"), v(m, "v1-"))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
      assertNull(client.get(k(m, "k2-")))
   }

   def testPrependBasic(m: Method) {
      addAndGet(m)
      val f = client.prepend(0, k(m), v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val expected = v(m, "v1-") + v(m)
      assertEquals(client.get(k(m)), expected)
   }

   def testPrependNotFound(m: Method) {
      addAndGet(m)
      val f = client.prepend(0, k(m, "k2-"), v(m, "v1-"))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
      assertNull(client.get(k(m, "k2-")))
   }

   def testGetsBasic(m: Method) {
      addAndGet(m)
      val value = client.gets(k(m))
      assertEquals(value.getValue(), v(m))
      assertTrue(value.getCas() != 0)
   }

   def testCasBasic(m: Method) {
      addAndGet(m)
      val value = client.gets(k(m))
      assertEquals(value.getValue(), v(m))
      assertTrue(value.getCas() != 0)
      val resp = client.cas(k(m), value.getCas, v(m, "v1-"))
      assertEquals(resp, CASResponse.OK)
   }

   def testCasNotFound(m: Method) {
      addAndGet(m)
      val value = client.gets(k(m))
      assertEquals(value.getValue(), v(m))
      assertTrue(value.getCas() != 0)
      val resp = client.cas(k(m, "k1-"), value.getCas, v(m, "v1-"))
      assertEquals(resp, CASResponse.NOT_FOUND)
   }

   def testCasExists(m: Method) {
      addAndGet(m)
      var value = client.gets(k(m))
      assertEquals(value.getValue(), v(m))
      assertTrue(value.getCas() != 0)
      val old = value.getCas
      var resp = client.cas(k(m), value.getCas, v(m, "v1-"))
      value = client.gets(k(m))
      assertEquals(value.getValue(), v(m, "v1-"))
      assertTrue(value.getCas() != 0)
      assertTrue(value.getCas() != old)
      resp = client.cas(k(m), old, v(m, "v2-"))
      assertEquals(resp, CASResponse.EXISTS)
      resp = client.cas(k(m), value.getCas, v(m, "v2-"))
      assertEquals(resp, CASResponse.OK)
   }

   def testInvalidCas {
      var resp = send("cas bad blah 0 0 0\r\n\r\n")
      assertClientError(resp)

      resp = send("cas bad 0 blah 0 0\r\n\r\n")
      assertClientError(resp)

      resp = send("cas bad 0 0 blah 0\r\n\r\n")
      assertClientError(resp)

      resp = send("cas bad 0 0 0 blah\r\n\r\n")
      assertClientError(resp)
   }

   def testInvalidCasValue {
      val resp = send("cas foo 0 0 6 \r\nbarva2\r\n")
      assertClientError(resp)
   }

   def testDeleteBasic(m: Method) {
      addAndGet(m)
      val f = client.delete(k(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertNull(client.get(k(m)))
   }

   def testDeleteDoesNotExist(m: Method) {
      val f = client.delete(k(m))
      assertFalse(f.get(timeout, TimeUnit.SECONDS).booleanValue)
   }

   def testPipelinedDelete {
      val responses = sendMulti("delete a\r\ndelete a\r\n", 2)
      assertEquals(responses.length, 2)
      responses.foreach(r => assertTrue(r == "NOT_FOUND"))
   }

   def testPipelinedGetAfterInvalidCas {
      val responses = sendMulti("cas bad 0 0 1 0 0\r\nget a\r\n", 2)
      assertEquals(responses.length, 2)
      assertTrue(responses.head.contains("CLIENT_ERROR"))
      assertTrue(responses.tail.head == "END", "Instead response was: " + responses.tail.head)
   }

   def testIncrementBasic(m: Method) {
      val f = client.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val result = client.incr(k(m), 1)
      assertEquals(result, 2)
   }

   def testIncrementTriple(m: Method) {
      val f = client.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.incr(k(m), 1), 2)
      assertEquals(client.incr(k(m), 2), 4)
      assertEquals(client.incr(k(m), 4), 8)
   }

   def testIncrementNotExist(m: Method) {
      assertEquals(client.incr(k(m), 1), -1)
   }

   def testIncrementIntegerMax(m: Method) {
      val f = client.set(k(m), 0, "0")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.incr(k(m), Integer.MAX_VALUE), Integer.MAX_VALUE)
   }

   def testIncrementBeyondIntegerMax(m: Method) {
      val f = client.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val newValue = client.incr(k(m), Integer.MAX_VALUE)
      assertEquals(newValue, Int.MaxValue.asInstanceOf[Long] + 1)
   }

   def testIncrementBeyondLongMax(m: Method) {
      val f = client.set(k(m), 0, "9223372036854775808")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val newValue = incr(m, 1)
      assertEquals(BigInt(newValue), BigInt("9223372036854775809"))
   }

   def testIncrementSurpassLongMax(m: Method) {
      val f = client.set(k(m), 0, "9223372036854775807")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val newValue = incr(m, 1)
      assertEquals(BigInt(newValue), BigInt("9223372036854775808"))
   }

   def testIncrementSurpassBigIntMax(m: Method) {
      val f = client.set(k(m), 0, "18446744073709551615")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val newValue = incr(m, 1)
      assertEquals(newValue, "0")
   }

   def testDecrementBasic(m: Method) {
      val f = client.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.decr(k(m), 1), 0)
   }

   def testDecrementTriple(m: Method) {
      val f = client.set(k(m), 0, "8")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.decr(k(m), 1), 7)
      assertEquals(client.decr(k(m), 2), 5)
      assertEquals(client.decr(k(m), 4), 1)
   }

   def testDecrementNotExist(m: Method): Unit = {
      assertEquals(client.decr(k(m), 1), -1)
   }

   def testDecrementBelowZero(m: Method) {
      val f = client.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val newValue = client.decr(k(m), 2)
      assertEquals(newValue, 0)
   }

   def testFlushAll(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         val f = client.set(key, 0, value);
         assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
         assertEquals(client.get(key), value)
      }

      val f = client.flush();
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         assertNull(client.get(key))
      }
   }

   def testFlushAllDelayed(m: Method) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         val f = client.set(key, 0, value);
         assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
         assertEquals(client.get(key), value)
      }

      val f = client.flush(2);
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)

      sleepThread(2200);

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         assertNull(client.get(key))
      }
   }

   def testVersion {
      val versions = client.getVersions
      assertEquals(versions.size(), 1)
      val version = versions.values.iterator.next
      assertEquals(version, Version.version)
   }

   def testIncrKeyLengthLimit {
      val keyUnderLimit = generateRandomString(249)
      var f = client.set(keyUnderLimit, 0, "78")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(keyUnderLimit), "78")

      val keyInLimit = generateRandomString(250)
      f = client.set(keyInLimit, 0, "89")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(keyInLimit), "89")

      val keyAboveLimit = generateRandomString(251)
      val resp = incr(keyAboveLimit, 1)
      assertClientError(resp)
   }

   def testGetKeyLengthLimit {
      var tooLongKey = generateRandomString(251)
      var resp = send("get " + tooLongKey + "\r\n")
      assertClientError(resp)

      tooLongKey = generateRandomString(251)
      resp = send("get k1 k2 k3 " + tooLongKey + "\r\n")
      assertClientError(resp)
   }

   def testUnknownCommand = assertError(send("blah\r\n"))

   def testReadFullLineAfterLongKey {
      val key = generateRandomString(300)
      val command = "add " + key + " 0 0 1\r\nget a\r\n"
      val responses = sendMulti(command, 2)
      assertEquals(responses.length, 2)
      assertTrue(responses.head.contains("CLIENT_ERROR"))
      assertTrue(responses.tail.head == "END", "Instead response was: " + responses.tail.head)
   }

   private def assertClientError(resp: String) = assertExpectedResponse(resp, "CLIENT_ERROR", false)

   private def assertError(resp: String) = assertExpectedResponse(resp, "ERROR", true)

   private def assertExpectedResponse(resp: String, expectedResp: String, strictComparison: Boolean) {
      if (strictComparison)
         assertEquals(resp, expectedResp, "Instead response is: " + resp)
      else
         assertTrue(resp.contains(expectedResp), "Instead response is: " + resp)
   }

//   def testRegex {
//      val notFoundRegex = new Regex("""\bNOT_FOUND\b""")
//      assertEquals(notFoundRegex.findAllIn("NOT_FOUND\r\nNOT_FOUND\r\n").length, 2)
//   }
//
//   private def assertExpectedResponse(resp: String, expectedResp: String, numberOfTimes: Int) {
//      val expectedRespRegex = new Regex("""\b""" + expectedResp + """\b""")
//      assertEquals(expectedRespRegex.findAllIn(resp).length, numberOfTimes,
//         "Expected " + expectedResp + " to be found " + numberOfTimes
//               + " times, but instead received response: " + resp)
//   }

   private def addAndGet(m: Method) {
      val f = client.add(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(client.get(k(m)), v(m))
   }

   private def incr(m: Method, by: Int): String = incr(k(m), by)

   private def incr(k: String, by: Int): String = send("incr " + k + " " + by + "\r\n")

   private def send(req: String): String = sendMulti(req, 1).head

   private def sendMulti(req: String, expectedResponses: Int): List[String] = {
      val socket = new Socket(server.getHost, server.getPort)
      try {
         socket.getOutputStream.write(req.getBytes)
         val buffer = new ListBuffer[String]
         for (i <- 0 until expectedResponses)
            buffer += readLine(socket.getInputStream, new StringBuilder)
         buffer.toList
      }
      finally {
         socket.close
      }
   }

   private def readLine(is: InputStream, sb: StringBuilder): String = {
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
}