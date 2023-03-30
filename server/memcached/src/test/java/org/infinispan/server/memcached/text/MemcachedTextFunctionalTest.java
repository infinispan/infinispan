package org.infinispan.server.memcached.text;

import static org.infinispan.test.TestingUtil.generateRandomString;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.logging.Log;
import org.infinispan.server.memcached.test.MemcachedFunctionalTest;
import org.testng.annotations.Test;

import net.spy.memcached.internal.OperationFuture;

/**
 * @since 15.0
 **/
@Test(groups = "functional", testName = "server.memcached.text.MemcachedTextFunctionalTest")
public class MemcachedTextFunctionalTest extends MemcachedFunctionalTest {
   @Override
   protected MemcachedProtocol getProtocol() {
      return MemcachedProtocol.TEXT;
   }

   @Override
   protected boolean withAuthentication() {
      return false;
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
      String resp = send("cas foo 0 0 6\r\nbarva2\r\n");
      assertClientError(resp);
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

   public void testPipelinedDelete() throws IOException {
      List<String> responses = sendMulti("delete a\r\ndelete a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      responses.forEach(r -> assertTrue(r.equals("NOT_FOUND")));
   }

   public void testPipelinedGetAfterInvalidCas() throws IOException {
      List<String> responses = sendMulti("cas bad 0 0 1 0 0\r\nget a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      assertTrue(responses.get(0), responses.get(0).contains("CLIENT_ERROR"));
      assertEquals("END", responses.get(1));
   }

   public void testFlushAllPipeline() throws IOException {
      List<String> responses = sendMulti("flush_all\r\nget a\r\n", 2, true);
      assertEquals(responses.size(), 2);
      assertEquals(responses.get(0), "OK");
      assertEquals(responses.get(1), "END");
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
      assertEquals("ERROR", responses.get(0));
      assertEquals("NOT_FOUND", responses.get(1));
   }

   public void testReadFullLineAfterLongKey() throws IOException {
      String key = generateRandomString(300);
      String command = "add " + key + " 0 0 1\r\nget a\r\n";
      List<String> responses = sendMulti(command, 2, true);
      assertEquals(2, responses.size());
      assertTrue(responses.get(0), responses.get(0).contains("CLIENT_ERROR"));
      assertEquals("END", responses.get(1));
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

   private String incr(Method m, int by) throws IOException {
      return incr(k(m), by);
   }

   private String incr(String k, int by) throws IOException {
      return send("incr " + k + " " + by + "\r\n");
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

   public void testBufferOverflowCausesUnknownException() throws Exception {
      List<String> keys = Files.readAllLines(
            Paths.get(getClass().getClassLoader().getResource("keys.txt").toURI()),
            StandardCharsets.UTF_8
      );

      for (String key : keys) {
         assertTrue(client.set(key, 0, "ISPN005003: UnknownOperationException").get());
      }
   }

   public void testDeleteNoReply(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      withNoReply(m, String.format("delete %s noreply\r\n", k(m)));
   }

   public void testFlushAllNoReply(Method m) throws InterruptedException, ExecutionException, TimeoutException, IOException {
      withNoReply(m, "flush_all noreply\r\n");
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

}
