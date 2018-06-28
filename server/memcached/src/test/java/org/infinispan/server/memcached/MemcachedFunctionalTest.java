package org.infinispan.server.memcached;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.infinispan.test.TestingUtil.generateRandomString;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.sleepThread;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.server.memcached.logging.Log;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

/**
 * Tests Memcached protocol functionality against Infinispan Memcached server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedFunctionalTest")
public class MemcachedFunctionalTest extends MemcachedSingleNodeTest {

   public void testSetBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }

   public void testSetWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m)));
   }

   public void testSetWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      OperationFuture<Boolean> f = client.set(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m)));
   }

   public void testSetWithExpiryUnixTimeInPast(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 60*60*24*30 + 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m)));
   }

   public void testSetWithUTF8Key(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      String key = "\u4f60\u597d-";
      OperationFuture<Boolean> f = client.set(k(m, key), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(v(m), client.get(k(m, key)));
   }

   public void testGetMultipleKeys(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f1 = client.set(k(m, "k1-"), 0, v(m, "v1-"));
      OperationFuture<Boolean> f2 = client.set(k(m, "k2-"), 0, v(m, "v2-"));
      OperationFuture<Boolean> f3 = client.set(k(m, "k3-"), 0, v(m, "v3-"));
      assertTrue(f1.get(timeout, TimeUnit.SECONDS));
      assertTrue(f2.get(timeout, TimeUnit.SECONDS));
      assertTrue(f3.get(timeout, TimeUnit.SECONDS));
      List<String> keys = Arrays.asList(k(m, "k1-"), k(m, "k2-"), k(m, "k3-"));
      Map<String, Object> ret = client.getBulk(keys);
      assertEquals(ret.get(k(m, "k1-")), v(m, "v1-"));
      assertEquals(ret.get(k(m, "k2-")), v(m, "v2-"));
      assertEquals(ret.get(k(m, "k3-")), v(m, "v3-"));
   }

   public void testAddBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
   }

   public void testAddWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.add(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m)));
      f = client.add(k(m), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
   }

   public void testAddWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      OperationFuture<Boolean> f = client.add(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m)));
      f = client.add(k(m), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
   }

   public void testNotAddIfPresent(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.add(k(m), 0, v(m, "v1-"));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }

   public void testReplaceBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.replace(k(m), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
   }

   public void testNotReplaceIfNotPresent(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.replace(k(m), 0, v(m));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      assertNull(client.get(k(m)));
   }

   public void testReplaceWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.replace(k(m), 1, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
      sleepThread(1100);
      assertNull(client.get(k(m)));
   }

   public void testReplaceWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      OperationFuture<Boolean> f = client.replace(k(m), future, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
      sleepThread(1100);
      assertNull(client.get(k(m)));
   }

   public void testAppendBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.append(0, k(m), v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String expected = v(m) + v(m, "v1-");
      assertEquals(client.get(k(m)), expected);
   }

   public void testAppendNotFound(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.append(0, k(m, "k2-"), v(m, "v1-"));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
      assertNull(client.get(k(m, "k2-")));
   }

   public void testPrependBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.prepend(0, k(m), v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String expected = v(m, "v1-") + v(m);
      assertEquals(client.get(k(m)), expected);
   }

   public void testPrependNotFound(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.prepend(0, k(m, "k2-"), v(m, "v1-"));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
      assertNull(client.get(k(m, "k2-")));
   }

   public void testGetsBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      CASValue value = client.gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
   }

   public void testCasBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      CASValue value = client.gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
      CASResponse resp = client.cas(k(m), value.getCas(), v(m, "v1-"));
      assertEquals(resp, CASResponse.OK);
   }

   public void testCasNotFound(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      CASValue value = client.gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
      CASResponse resp = client.cas(k(m, "k1-"), value.getCas(), v(m, "v1-"));
      assertEquals(resp, CASResponse.NOT_FOUND);
   }

   public void testCasExists(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      CASValue value = client.gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
      long old = value.getCas();
      CASResponse resp = client.cas(k(m), value.getCas(), v(m, "v1-"));
      value = client.gets(k(m));
      assertEquals(value.getValue(), v(m, "v1-"));
      assertTrue(value.getCas() != 0);
      assertTrue(value.getCas() != old);
      resp = client.cas(k(m), old, v(m, "v2-"));
      assertEquals(resp, CASResponse.EXISTS);
      resp = client.cas(k(m), value.getCas(), v(m, "v2-"));
      assertEquals(resp, CASResponse.OK);
   }

   public void testInvalidCas() throws IOException {
      String resp = send("cas bad blah 0 0 0\r\n\r\n");
      assertClientError(resp);

      resp = send("cas bad 0 blah 0 0\r\n\r\n");
      assertClientError(resp);

      resp = send("cas bad 0 0 blah 0\r\n\r\n");
      assertClientError(resp);

      resp = send("cas bad 0 0 0 blah\r\n\r\n");
      assertClientError(resp);
   }

   public void testInvalidCasValue() throws IOException {
      String resp = send("cas foo 0 0 6 \r\nbarva2\r\n");
      assertClientError(resp);
   }

   public void testDeleteBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      OperationFuture<Boolean> f = client.delete(k(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertNull(client.get(k(m)));
   }

   public void testDeleteDoesNotExist(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.delete(k(m));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
   }

   public void testDeleteNoReply(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      withNoReply(m, String.format("delete %s noreply\r\n", k(m)));
   }

   public void testSetAndMultiDelete(Method m) throws IOException {
      String key = k(m);
      List<String> responses = sendMulti(String.format(
              "set %s 0 0 1\r\na\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\n", key, key, key, key, key), 5, true);
      assertEquals(responses.size(), 5);
      assertEquals(responses.get(0), "STORED");
      assertEquals(responses.get(1), "DELETED");
      assertEquals(responses.get(2), "NOT_FOUND");
      assertEquals(responses.get(3), "NOT_FOUND");
      assertEquals(responses.get(4), "NOT_FOUND");
   }

   public void testSetNoReplyMultiDelete(Method m) throws IOException {
      String key = k(m);
      List<String> responses = sendMulti(String.format(
         "set %s 0 0 1 noreply\r\na\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\ndelete %s\r\n", key, key, key, key, key), 4, true);
      assertEquals(responses.size(), 4);
      assertEquals(responses.get(0), "DELETED");
      assertEquals(responses.get(1), "NOT_FOUND");
      assertEquals(responses.get(2), "NOT_FOUND");
      assertEquals(responses.get(3), "NOT_FOUND");
   }

   private void withNoReply(Method m, String op) throws InterruptedException, ExecutionException, TimeoutException,
           IOException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "blah");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      CountDownLatch latch = new CountDownLatch(1);
      NoReplyListener listener = new NoReplyListener(latch);
      cache.addListener(listener);
      try {
         sendNoWait(op);
         log.debug("No reply delete sent, wait...");
         boolean completed = latch.await(10, TimeUnit.SECONDS);
         assertTrue("Timed out waiting for remove to be executed", completed);
      } finally {
         cache.removeListener(listener);
      }
   }

   public void testPipelinedDelete() throws IOException {
      List<String> responses = sendMulti("delete a\r\ndelete a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      responses.forEach(r -> assertTrue(r.equals("NOT_FOUND")));
   }

   public void testPipelinedGetAfterInvalidCas() throws IOException {
      List<String> responses = sendMulti("cas bad 0 0 1 0 0\r\nget a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      assertTrue(responses.get(0).contains("CLIENT_ERROR"));
      assertTrue("Instead response was: " + responses.get(1), responses.get(1).equals("END"));
   }

   public void testIncrementBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      long result = client.incr(k(m), 1);
      assertEquals(result, 2);
   }

   public void testIncrementTriple(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.incr(k(m), 1), 2);
      assertEquals(client.incr(k(m), 2), 4);
      assertEquals(client.incr(k(m), 4), 8);
   }

   public void testIncrementNotExist(Method m) {
      assertEquals(client.incr(k(m), 1), -1);
   }

   public void testIncrementIntegerMax(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "0");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.incr(k(m), Integer.MAX_VALUE), Integer.MAX_VALUE);
   }

   public void testIncrementBeyondIntegerMax(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      long newValue = client.incr(k(m), Integer.MAX_VALUE);
      assertEquals(newValue, (long) Integer.MAX_VALUE + 1);
   }

   public void testIncrementBeyondLongMax(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "9223372036854775808");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String newValue = incr(m, 1);
      assertEquals(new BigInteger(newValue), new BigInteger("9223372036854775809"));
   }

   public void testIncrementSurpassLongMax(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "9223372036854775807");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String newValue = incr(m, 1);
      assertEquals(new BigInteger(newValue), new BigInteger("9223372036854775808"));
   }

   public void testIncrementSurpassBigIntMax(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "18446744073709551615");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String newValue = incr(m, 1);
      assertEquals(newValue, "0");
   }

   public void testDecrementBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.decr(k(m), 1), 0);
   }

   public void testDecrementTriple(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "8");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.decr(k(m), 1), 7);
      assertEquals(client.decr(k(m), 2), 5);
      assertEquals(client.decr(k(m), 4), 1);
   }

   public void testDecrementNotExist(Method m) {
      assertEquals(client.decr(k(m), 1), -1);
   }

   public void testDecrementBelowZero(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      long newValue = client.decr(k(m), 2);
      assertEquals(newValue, 0);
   }

   public void testFlushAll(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      for (int i = 1; i < 5; ++i) {
         String key = k(m, "k" + i + "-");
         String value = v(m, "v" + i + "-");
         OperationFuture<Boolean> f = client.set(key, 0, value);
         assertTrue(f.get(timeout, TimeUnit.SECONDS));
         assertEquals(client.get(key), value);
      }

      OperationFuture<Boolean> f = client.flush();
      assertTrue(f.get(timeout, TimeUnit.SECONDS));

      for (int i = 1; i < 5; ++i) {
         String key = k(m, "k" + i + "-");
         assertNull(client.get(key));
      }
   }

   public void testFlushAllDelayed(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      flushAllDelayed(m, 2, 2200);
   }

   public void testFlushAllDelayedUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int delay = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 2000);
      flushAllDelayed(m, delay, 2200);
   }

   private void flushAllDelayed(Method m, int delay, long sleep) throws InterruptedException, ExecutionException, TimeoutException {
      for (int i = 1; i < 5; ++i) {
         String key = k(m, "k" + i + "-");
         String value = v(m, "v" + i + "-");
         OperationFuture<Boolean> f = client.set(key, 0, value);
         assertTrue(f.get(timeout, TimeUnit.SECONDS));
         assertEquals(client.get(key), value);
      }

      OperationFuture<Boolean> f = client.flush(delay);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));

      sleepThread(sleep);

      for (int i = 1; i < 5; ++i) {
         String key = k(m, "k" + i + "-");
         assertNull(client.get(key));
      }
   }

   public void testFlushAllNoReply(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
     withNoReply(m, "flush_all noreply\r\n");
   }

   public void testFlushAllPipeline() throws IOException {
      List<String> responses = sendMulti("flush_all\r\nget a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      assertEquals(responses.get(0), "OK");
      assertEquals(responses.get(1), "END");
   }

   public void testVersion() {
      Map<SocketAddress, String> versions = client.getVersions();
      assertEquals(versions.size(), 1);
      String version = versions.values().iterator().next();
      assertEquals(version, Version.getVersion());
   }

   public void testIncrKeyLengthLimit() throws InterruptedException, ExecutionException, TimeoutException, IOException {
      String keyUnderLimit = generateRandomString(249);
      OperationFuture<Boolean> f = client.set(keyUnderLimit, 0, "78");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(keyUnderLimit), "78");

      String keyInLimit = generateRandomString(250);
      f = client.set(keyInLimit, 0, "89");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(keyInLimit), "89");

      String keyAboveLimit = generateRandomString(251);
      String resp = incr(keyAboveLimit, 1);
      assertClientError(resp);
   }

   public void testGetKeyLengthLimit() throws IOException {
      String tooLongKey = generateRandomString(251);
      String resp = send("get " + tooLongKey + "\r\n");
      assertClientError(resp);

      tooLongKey = generateRandomString(251);
      resp = send("get k1 k2 k3 " + tooLongKey + "\r\n");
      assertClientError(resp);
   }

   public void testUnknownCommand() throws IOException {
      assertError(send("blah\r\n"));
      assertError(send("blah boo poo goo zoo\r\n"));
   }

   public void testUnknownCommandPipelined() throws IOException {
      List<String> responses = sendMulti("bogus\r\ndelete a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      assertEquals(responses.get(0), "ERROR");
      assertEquals(responses.get(1), "NOT_FOUND");
   }

   public void testReadFullLineAfterLongKey() throws IOException {
      String key = generateRandomString(300);
      String command = "add " + key + " 0 0 1\r\nget a\r\n";
      List<String> responses = sendMulti(command, 2, true);
      assertEquals(responses.size(), 2);
      assertTrue(responses.get(0).contains("CLIENT_ERROR"));
      assertEquals(responses.get(1), "END");
   }

   public void testNegativeBytesLengthValue() throws IOException {
      assertClientError(send("set boo1 0 0 -1\r\n"));
      assertClientError(send("add boo2 0 0 -1\r\n"));
   }

   public void testFlagsIsUnsigned(Method m) throws IOException {
      String k = m.getName();
      assertClientError(send("set boo1 -1 0 0\r\n"));
      assertStored(send("set " + k + " 4294967295 0 0\r\n\r\n"));
      assertClientError(send("set boo2 4294967296 0 0\r\n"));
      assertClientError(send("set boo2 18446744073709551615 0 0\r\n"));
   }

   public void testIncrDecrIsUnsigned(Method m) throws IOException, InterruptedException, ExecutionException, TimeoutException {
      String k = m.getName();
      OperationFuture<Boolean> f = client.set(k, 0, "0");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertClientError(send("incr " + k + " -1\r\n"));
      assertClientError(send("decr " + k + " -1\r\n"));
      k = k + "-1";
      f = client.set(k, 0, "0");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertExpectedResponse(send("incr " + k + " 18446744073709551615\r\n"), "18446744073709551615", true);
      k = k + "-1";
      f = client.set(k, 0, "0");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertClientError(send("incr " + k + " 18446744073709551616\r\n"));
      assertClientError(send("decr " + k + " 18446744073709551616\r\n"));
   }

   public void testVerbosity() throws IOException {
      assertClientError(send("verbosity\r\n"));
      assertClientError(send("verbosity 5\r\n"));
      assertClientError(send("verbosity 10 noreply\r\n"));
   }

   public void testQuit(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      OperationFuture<Boolean> f = client.set(k(m), 0, "0");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sendNoWait("quit\r\n");
   }

   public void testSetBigSizeValue(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, generateRandomString(1024 * 1024).getBytes());
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
   }

   public void testStoreAsBinaryOverride() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.memory().storageType(StorageType.BINARY);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      Configuration cfg = builder.build();
      cm.defineConfiguration(new MemcachedServerConfigurationBuilder().build().defaultCacheName(), cfg);
      assertEquals(StorageType.BINARY, cfg.memory().storageType());
      MemcachedServer testServer = startMemcachedTextServer(cm, server.getPort() + 33);
      try {
         Cache memcachedCache = cm.getCache(testServer.getConfiguration().defaultCacheName());
         assertEquals(StorageType.BINARY, memcachedCache.getCacheConfiguration().memory().storageType());
      } finally {
         cm.stop();
         testServer.stop();
      }
   }

   public void testDisableCache(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(v(m), client.get(k(m)));

      String cacheName = server.getConfiguration().defaultCacheName();

      server.ignoreCache(cacheName);
      try {
         client.get(k(m));
         fail("Should have failed");
      } catch (Exception e) {
         // Ignore, should fail
      }


      server.unignore(cacheName);
         client.get(k(m));
   }

   public void testBufferOverflowCausesUnknownException() throws Exception {
      List<String> keys = Files.readAllLines(
         Paths.get(getClass().getClassLoader().getResource("keys.txt").toURI()),
         StandardCharsets.UTF_8
      );

      for (String key : keys) {
         assertTrue(client.set(key, 0, "ISPN005003: UnknownOperationException").get());
      }
   }

   private void addAndGet(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }

   private String incr(Method m, int by) throws IOException {
      return incr(k(m), by);
   }

   private String incr(String k, int by) throws IOException {
      return send("incr " + k + " " + by + "\r\n");
   }

}

@Listener
class NoReplyListener {
   private final CountDownLatch latch;

   Log log = LogFactory.getLog(NoReplyListener.class, Log.class);

   NoReplyListener(CountDownLatch latch) {
      this.latch = latch;
   }

   @CacheEntryRemoved
   public void removed(CacheEntryRemovedEvent event) {
      log.debug("Entry removed, open latch");
      latch.countDown();
   }

}
