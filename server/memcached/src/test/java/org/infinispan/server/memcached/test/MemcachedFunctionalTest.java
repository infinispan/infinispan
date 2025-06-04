package org.infinispan.server.memcached.test;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.sleepThread;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.util.Version;
import org.testng.annotations.Test;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

/**
 * Tests Memcached protocol functionality against Infinispan Memcached server.
 */
@Test(groups = "functional", testName = "server.memcached.test.MemcachedFunctionalTest")
public abstract class MemcachedFunctionalTest extends MemcachedSingleNodeTest {

   public void testSetBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }

   public void testSetWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testSetWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 1000);
      OperationFuture<Boolean> f = client.set(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testSetWithExpiryUnixTimeInPast(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 60 * 60 * 24 * 30 + 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
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
      assertEquals(v(m, "v1-"), ret.get(k(m, "k1-")));
      assertEquals(v(m, "v2-"), ret.get(k(m, "k2-")));
      assertEquals(v(m, "v3-"), ret.get(k(m, "k3-")));
   }

   public void testAddBasic(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
   }

   public void testTouchWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 1000);
      OperationFuture<Boolean> f = client.set(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      f = client.touch(k(m), future + 1);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertEquals(v(m), client.get(k(m)));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testTouchWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      f = client.touch(k(m), 2);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertEquals(v(m), client.get(k(m)));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testTouchMiss(Method m) throws ExecutionException, InterruptedException, TimeoutException {
      OperationFuture<Boolean> f = client.touch(k(m), 1);
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
   }

   public void testGetAndTouchWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(v(m), client.getAndTouch(k(m), 2).getValue());
      timeService.advance(1100);
      assertEquals(v(m), client.get(k(m)));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testGetAndTouchWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 1000);
      OperationFuture<Boolean> f = client.set(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(v(m), client.getAndTouch(k(m), future + 1).getValue());
      timeService.advance(1100);
      assertEquals(v(m), client.get(k(m)));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testTouch0(Method m) throws ExecutionException, InterruptedException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 1, "Value");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals("Value", client.get(k(m)));
      f = client.touch(k(m), 0);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertEquals("Value", client.get(k(m)));
      f = client.touch(k(m), 3);
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
   }

   public void testGetAndTouchMiss(Method m) {
      CASValue<Object> v = client.getAndTouch(k(m), 1);
      assertNull(v);
   }

   public void testAddWithExpirySeconds(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.add(k(m), 1, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
      assertNull(client.get(k(m)));
      f = client.add(k(m), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
   }

   public void testAddWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 1000);
      OperationFuture<Boolean> f = client.add(k(m), future, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      timeService.advance(1100);
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
      timeService.advance(1100);
      assertNull(client.get(k(m)));
   }

   public void testReplaceWithExpiryUnixTime(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      addAndGet(m);
      int future = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 1000);
      OperationFuture<Boolean> f = client.replace(k(m), future, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m, "v1-"));
      timeService.advance(1100);
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
      long old = value.getCas();
      assertTrue(old != 0);
      client.cas(k(m), old, v(m, "v1-"));
      value = client.gets(k(m));
      assertEquals(v(m, "v1-"), value.getValue());
      assertTrue(value.getCas() != 0);
      assertTrue(value.getCas() != old);
      CASResponse resp = client.cas(k(m), old, v(m, "v2-"));
      assertEquals(CASResponse.EXISTS, resp);
      resp = client.cas(k(m), value.getCas(), v(m, "v2-"));
      assertEquals(CASResponse.OK, resp);
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
      assertEquals(-1, client.incr(k(m), 1));
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
      int delay = (int) TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() + 2000);
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

      // The underlying ScheduledExecutorService does not use the ControlledTimeService unfortunately, so we need to sleep
      sleepThread(sleep);
      timeService.advance(sleep);

      for (int i = 1; i < 5; ++i) {
         String key = k(m, "k" + i + "-");
         assertNull(client.get(key));
      }
   }

   public void testVersion() {
      Map<SocketAddress, String> versions = client.getVersions();
      assertEquals(versions.size(), 1);
      String version = versions.values().iterator().next();
      assertEquals(Version.getVersion(), version);
   }

   private void addAndGet(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }
}
