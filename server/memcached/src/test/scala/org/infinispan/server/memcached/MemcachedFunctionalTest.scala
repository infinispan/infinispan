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
package org.infinispan.server.memcached

import java.lang.reflect.Method
import logging.Log
import org.testng.Assert._
import org.testng.annotations.Test
import net.spy.memcached.CASResponse
import org.infinispan.test.TestingUtil._
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.config.Configuration
import org.infinispan.Version
import test.MemcachedTestingUtil._
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder

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

   def testDeleteNoReply(m: Method) {
      withNoReply(m, "delete %s noreply\r\n".format(k(m)))
   }

   def testSetAndMultiDelete(m: Method) {
      val key = k(m)
      val responses = sendMulti(
         "set %s 0 0 1\r\na\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\n"
                 .format(key, key, key, key, key), 5, true)
      assertEquals(responses.length, 5)
      assertEquals(responses.head, "STORED")
      assertEquals(responses.tail.head, "DELETED")
      assertEquals(responses.tail.tail.head, "NOT_FOUND")
      assertEquals(responses.tail.tail.tail.head, "NOT_FOUND")
      assertEquals(responses.tail.tail.tail.tail.head, "NOT_FOUND")
   }

   def testSetNoReplyMultiDelete(m: Method) {
      val key = k(m)
      val responses = sendMulti(
         "set %s 0 0 1 noreply\r\na\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\n"
                 .format(key, key, key, key, key), 4, true)
      assertEquals(responses.length, 4)
      assertEquals(responses.head, "DELETED")
      assertEquals(responses.tail.head, "NOT_FOUND")
      assertEquals(responses.tail.tail.head, "NOT_FOUND")
      assertEquals(responses.tail.tail.tail.head, "NOT_FOUND")
   }

   private def withNoReply(m: Method, op: String) {
      val f = client.set(k(m), 0, "blah")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val latch = new CountDownLatch(1)
      val listener = new NoReplyListener(latch)
      cache.addListener(listener)
      try {
         sendNoWait(op)
         log.debug("No reply delete sent, wait...")
         val completed = latch.await(10, TimeUnit.SECONDS)
         assertTrue(completed, "Timed out waiting for remove to be executed")
      } finally {
         cache.removeListener(listener)
      }
   }

   def testPipelinedDelete {
      val responses = sendMulti("delete a\r\ndelete a\r\n", 2, true)
      assertEquals(responses.length, 2)
      responses.foreach(r => assertTrue(r == "NOT_FOUND"))
   }

   def testPipelinedGetAfterInvalidCas {
      val responses = sendMulti("cas bad 0 0 1 0 0\r\nget a\r\n", 2, true)
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

   def testFlushAllDelayed(m: Method) = flushAllDelayed(m, 2, 2200)

   def testFlushAllDelayedUnixTime(m: Method) {
      val delay: Int = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis + 2000).asInstanceOf[Int]
      flushAllDelayed(m, delay, 2200)
   }

   private def flushAllDelayed(m: Method, delay: Int, sleep: Long) {
      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         val value = v(m, "v" + i + "-");
         val f = client.set(key, 0, value);
         assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
         assertEquals(client.get(key), value)
      }

      val f = client.flush(delay);
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)

      sleepThread(sleep);

      for (i <- 1 to 5) {
         val key = k(m, "k" + i + "-");
         assertNull(client.get(key))
      }
   }

   def testFlushAllNoReply(m: Method) {
     withNoReply(m, "flush_all noreply\r\n")
   }

   def testFlushAllPipeline {
      val responses = sendMulti("flush_all\r\nget a\r\n", 2, true)
      assertEquals(responses.length, 2)
      assertEquals(responses.head, "OK")
      assertEquals(responses.tail.head, "END")
   }

   def testVersion {
      val versions = client.getVersions
      assertEquals(versions.size(), 1)
      val version = versions.values.iterator.next
      assertEquals(version, Version.VERSION)
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

   def testUnknownCommand {
      assertError(send("blah\r\n"))
      assertError(send("blah boo poo goo zoo\r\n"))
   }

   def testUnknownCommandPipelined {
      val responses = sendMulti("bogus\r\ndelete a\r\n", 2, true)
      assertEquals(responses.length, 2)
      assertEquals(responses.head, "ERROR")
      assertEquals(responses.tail.head, "NOT_FOUND")
   }

   def testReadFullLineAfterLongKey {
      val key = generateRandomString(300)
      val command = "add " + key + " 0 0 1\r\nget a\r\n"
      val responses = sendMulti(command, 2, true)
      assertEquals(responses.length, 2)
      assertTrue(responses.head.contains("CLIENT_ERROR"))
      assertEquals(responses.tail.head, "END")
   }

   def testNegativeBytesLengthValue {
      assertClientError(send("set boo1 0 0 -1\r\n"))
      assertClientError(send("add boo2 0 0 -1\r\n"))
   }

   def testFlagsIsUnsigned(m: Method) {
      val k = m.getName
      assertClientError(send("set boo1 -1 0 0\r\n"))
      assertStored(send("set " + k + " 4294967295 0 0\r\n"))
      assertClientError(send("set boo2 4294967296 0 0\r\n"))
      assertClientError(send("set boo2 18446744073709551615 0 0\r\n"))
   }

   def testIncrDecrIsUnsigned(m: Method) {
      var k = m.getName
      var f = client.set(k, 0, "0")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertClientError(send("incr " + k + " -1\r\n"))
      assertClientError(send("decr " + k + " -1\r\n"))
      k = k + "-1"
      f = client.set(k, 0, "0")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertExpectedResponse(send("incr " + k + " 18446744073709551615\r\n"), "18446744073709551615", true)
      k = k + "-1"
      f = client.set(k, 0, "0")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertClientError(send("incr " + k + " 18446744073709551616\r\n"))
      assertClientError(send("decr " + k + " 18446744073709551616\r\n"))
   }

   def testVerbosity {
      assertClientError(send("verbosity\r\n"))
      assertClientError(send("verbosity 5\r\n"))
      assertClientError(send("verbosity 10 noreply\r\n"))
   }

   def testQuit(m: Method) {
      val f = client.set(k(m), 0, "0")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      sendNoWait("quit\r\n")
   }

   def testSetBigSizeValue(m: Method) {
      val f = client.set(k(m), 0, generateRandomString(1024 * 1024).getBytes)
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
   }

   def testStoreAsBinaryOverride {
      val cm = TestCacheManagerFactory.createLocalCacheManager(false)
      val cfg = new Configuration().fluent.storeAsBinary.build
      cm.defineConfiguration(new MemcachedServerConfigurationBuilder().build.cache, cfg)
      assertTrue(cfg.isStoreAsBinary)
      val testServer = startMemcachedTextServer(cm, server.getPort + 33)
      try {
         val memcachedCache = cm.getCache(testServer.getConfiguration.cache)
         assertFalse(memcachedCache.getConfiguration.isStoreAsBinary)
      } finally {
         cm.stop
         testServer.stop
      }
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

}

@Listener
class NoReplyListener(latch: CountDownLatch) extends Log {

   @CacheEntryRemoved
   def removed(event: CacheEntryRemovedEvent[AnyRef, AnyRef]) {
      debug("Entry removed, open latch")
      latch.countDown
   }

}