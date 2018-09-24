package org.infinispan.api;

import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.AsyncAPITest")
public class AsyncAPITest extends SingleCacheManagerTest {

   private Cache<String, String> c;
   private ControlledTimeService timeService = new ControlledTimeService();
   private Long startTime;

   @BeforeMethod
   public void clearCache() {
      c.clear();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      c = cm.getCache();
      return cm;
   }

   public void testGetAsyncWhenKeyIsNotPresent() throws Exception {
      CompletableFuture<String> f = c.getAsync("k");
      assertFutureResult(f, null);
      assertNull(c.get("k"));
   }

   public void testGetAsyncAfterPut() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.getAsync("k");
      assertFutureResult(f, "v");
   }

   public void testGetAllAsync() throws Exception {
      c.put("key-one-get", "one");
      c.put("key-two-get", "two");
      c.put("key-three-get", "three");
      Set<String> keys = new HashSet<>();
      keys.add("key-one-get");
      keys.add("key-two-get");
      keys.add("key-three-get");
      CompletableFuture<Map<String, String>> getAllF = c.getAllAsync(keys);
      assertNotNull(getAllF);
      assertFalse(getAllF.isCancelled());
      Map<String, String> resultAsMap = getAllF.get();
      assertNotNull(resultAsMap);
      assertEquals("one", resultAsMap.get("key-one-get"));
      assertEquals("two", resultAsMap.get("key-two-get"));
      assertEquals("three", resultAsMap.get("key-three-get"));
      assertTrue(getAllF.isDone());
   }

   public void testPutAsync() throws Exception {
      CompletableFuture<String> f = c.putAsync("k", "v1");
      assertFutureResult(f, null);
      assertEquals("v1", c.get("k"));

      f = c.putAsync("k", "v2");
      assertFutureResult(f, "v1");
      assertEquals("v2", c.get("k"));
   }

   public void testPutAllAsyncSingleKeyValue() throws Exception {
      CompletableFuture<Void> f = c.putAllAsync(Collections.singletonMap("k", "v"));
      assertFutureResult(f, null);
      assertEquals("v", c.get("k"));
   }

   public void testPutAllAsyncMultipleKeyValue() throws Exception {
      Map<String, String> map = new HashMap<>();
      map.put("one-key", "one");
      map.put("two-key", "two");
      CompletableFuture<Void> putAllF = c.putAllAsync(map);
      assertFutureResult(putAllF, null);
      assertEquals("one", c.get("one-key"));
      assertEquals("two", c.get("two-key"));
   }

   public void testPutIfAbsentAsync() throws Exception {
      CompletableFuture<String> f = c.putIfAbsentAsync("k", "v1");
      assertFutureResult(f, null);
      assertEquals("v1", c.get("k"));

      f = c.putIfAbsentAsync("k", "v2");
      assertFutureResult(f, "v1");
      assertEquals("v1", c.get("k"));
   }

   public void testRemoveAsync() throws Exception {
      c.put("k", "v");
      assertEquals("v", c.get("k"));

      CompletableFuture<String> f = c.removeAsync("k");
      assertFutureResult(f, "v");
      assertNull(c.get("k"));
   }

   public void testRemoveConditionalAsync() throws Exception {
      c.put("k", "v");
      Future<Boolean> f = c.removeAsync("k", "v_nonexistent");
      assertFutureResult(f, false);
      assertEquals("v", c.get("k"));

      f = c.removeAsync("k", "v");
      assertFutureResult(f, true);
      assertNull(c.get("k"));
   }

   public void testReplaceAsyncNonExistingKey() throws Exception {
      CompletableFuture<String> f = c.replaceAsync("k", "v");
      assertFutureResult(f, null);
      assertNull(c.get("k"));
   }

   public void testReplaceAsyncExistingKey() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.replaceAsync("k", "v2");
      assertFutureResult(f, "v");
      assertEquals("v2", c.get("k"));
   }

   public void testReplaceAsyncConditionalOnOldValueNonExisting() throws Exception {
      c.put("k", "v");
      CompletableFuture<Boolean> f = c.replaceAsync("k", "v_nonexistent", "v2");
      assertFutureResult(f, false);
      assertEquals("v", c.get("k"));
   }

   public void testReplaceAsyncConditionalOnOldValue() throws Exception {
      c.put("k", "v");
      CompletableFuture<Boolean> f = c.replaceAsync("k", "v", "v2");
      assertFutureResult(f, true);
      assertEquals("v2", c.get("k"));
   }

   public void testComputeIfAbsentAsync() throws Exception {
      Function<Object, String> mappingFunction = k -> k + " world";
      assertEquals("hello world", c.computeIfAbsentAsync("hello", mappingFunction).get());
      assertEquals("hello world", c.get("hello"));

      Function<Object, String> functionAfterPut = k -> k + " happy";
      // hello already exists so nothing should happen
      assertEquals("hello world", c.computeIfAbsentAsync("hello", functionAfterPut).get());
      assertEquals("hello world", c.get("hello"));

      int cacheSizeBeforeNullValueCompute = c.size();
      Function<Object, String> functionMapsToNull = k -> null;
      assertNull("with function mapping to null returns null", c.computeIfAbsentAsync("kaixo", functionMapsToNull).get());
      assertNull("the key does not exist", c.get("kaixo"));
      assertEquals(cacheSizeBeforeNullValueCompute, c.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      Function<Object, String> functionMapsToException = k -> {
         throw computeRaisedException;
      };
      expectException(ExecutionException.class, RuntimeException.class, "hi there", () -> c.computeIfAbsentAsync("es", functionMapsToException).get());
   }

   public void testComputeIfPresentAsync() throws Exception {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      c.put("es", "hola");

      assertEquals("hello_es:hola", c.computeIfPresentAsync("es", mappingFunction).get());
      assertEquals("hello_es:hola", c.get("es"));

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(ExecutionException.class, RuntimeException.class, "hi there", () -> c.computeIfPresentAsync("es", mappingToException).get());

      BiFunction<Object, Object, String> mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertNull("unexisting key should return null", c.computeIfPresentAsync("fr", mappingForNotPresentKey).get());
      assertNull("unexisting key should return null", c.get("fr"));

      BiFunction<Object, Object, String> mappingToNull = (k, v) -> null;
      assertNull("mapping to null returns null", c.computeIfPresentAsync("es", mappingToNull).get());
      assertNull("the key is removed", c.get("es"));
   }

   public void testComputeAsync() throws Exception {
      BiFunction<Object, Object, String> mappingFunction = (k, v) -> "hello_" + k + ":" + v;
      c.put("es", "hola");

      assertEquals("hello_es:hola", c.computeAsync("es", mappingFunction).get());
      assertEquals("hello_es:hola", c.get("es"));

      BiFunction<Object, Object, String> mappingForNotPresentKey = (k, v) -> "absent_" + k + ":" + v;
      assertEquals("absent_fr:null", c.computeAsync("fr", mappingForNotPresentKey).get());
      assertEquals("absent_fr:null", c.get("fr"));

      BiFunction<Object, Object, String> mappingToNull = (k, v) -> null;
      assertNull("mapping to null returns null", c.computeAsync("es", mappingToNull).get());
      assertNull("the key is removed", c.get("es"));

      int cacheSizeBeforeNullValueCompute = c.size();
      assertNull("mapping to null returns null", c.computeAsync("eus", mappingToNull).get());
      assertNull("the key does not exist", c.get("eus"));
      assertEquals(cacheSizeBeforeNullValueCompute, c.size());

      RuntimeException computeRaisedException = new RuntimeException("hi there");
      BiFunction<Object, Object, String> mappingToException = (k, v) -> {
         throw computeRaisedException;
      };
      expectException(ExecutionException.class, RuntimeException.class, "hi there", () -> c.computeAsync("es", mappingToException).get());
   }

   public void testMergeAsync() throws Exception {
      c.put("k", "v");

      // replace
      c.mergeAsync("k", "v", (oldValue, newValue) -> "" + oldValue + newValue).get();
      assertEquals("vv", c.get("k"));

      // remove if null value after remapping
      c.mergeAsync("k", "v2", (oldValue, newValue) -> null).get();
      assertEquals(null, c.get("k"));

      // put if absent
      c.mergeAsync("k2", "42", (oldValue, newValue) -> "" + oldValue + newValue).get();
      assertEquals("42", c.get("k2"));

      c.put("k", "v");
      RuntimeException mergeRaisedException = new RuntimeException("hi there");
      expectException(ExecutionException.class, RuntimeException.class, "hi there", () -> cache.mergeAsync("k", "v1", (k, v) -> {
         throw mergeRaisedException;
      }).get());
   }

   public void testPutAsyncWithLifespanAndMaxIdle() throws Exception {
      // lifespan only
      Future<String> f = c.putAsync("k", "v", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 1000, 500, true);

      log.warn("STARTING FAILING ONE");

      // lifespan and max idle (test max idle)
      f = c.putAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 1000, 500, false);

      // lifespan and max idle (test lifespan)
      f = c.putAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 3000, 500, true);
   }

   public void testPutAllAsyncWithLifespanAndMaxIdle() throws Exception {
      // putAll lifespan only
      Future<Void> f = c.putAllAsync(Collections.singletonMap("k", "v1"), 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v1", 1000, 500, true);

      // putAll lifespan and max idle (test max idle)
      f = c.putAllAsync(Collections.singletonMap("k", "v2"), 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v2", 1000, 500, false);

      // putAll lifespan and max idle (test lifespan)
      f = c.putAllAsync(Collections.singletonMap("k", "v3"), 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v3", 3000, 500, true);
   }

   public void testPutIfAbsentAsyncWithLifespanAndMaxIdle() throws Exception {
      // putIfAbsent lifespan only
      c.put("k", "v1");
      CompletableFuture<String> f = c.putIfAbsentAsync("k", "v2", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v1");
      assertEquals("v1", c.get("k"));
      Thread.sleep(300);
      assertEquals("v1", c.get("k"));
      assertEquals("v1", c.remove("k"));
      assertNull(c.get("k"));

      // now really put (k removed) lifespan only
      f = c.putIfAbsentAsync("k", "v", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 1000, 500, true);

      // putIfAbsent lifespan and max idle (test max idle)
      f = c.putIfAbsentAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 1000, 500, false);

      // putIfAbsent lifespan and max idle (test lifespan)
      f = c.putIfAbsentAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      verifyEviction("k", "v", 3000, 500, true);
   }

   public void testReplaceAsyncWithLifespan() throws Exception {

      CompletableFuture<String> f = c.replaceAsync("k", "v", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, null);
      assertNull(c.get("k"));

      c.put("k", "v");
      f = c.replaceAsync("k", "v1", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v");
      verifyEviction("k", "v1", 1000, 500, true);

      //replace2
      c.put("k", "v1");
      Future<Boolean> f3 = c.replaceAsync("k", "v_nonexistent", "v2", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f3, false);
      Thread.sleep(300);
      assertEquals("v1", c.get("k"));

      f3 = c.replaceAsync("k", "v1", "v2", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f3, true);
      verifyEviction("k", "v2", 1000, 500, true);
   }

   public void testReplaceAsyncWithLifespanAndMaxIdle() throws Exception {

      // replace lifespan and max idle (test max idle)
      c.put("k", "v");
      CompletableFuture f = c.replaceAsync("k", "v1", 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v");
      verifyEviction("k", "v1", 1000, 500, false);

      // replace lifespan and max idle (test lifespan)
      c.put("k", "v");
      f = c.replaceAsync("k", "v1", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v");
      verifyEviction("k", "v1", 3000, 500, true);

      //replace2 ifespan and max idle (test max idle)
      c.put("k", "v1");
      f = c.replaceAsync("k", "v1", "v2", 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, true);
      verifyEviction("k", "v2", 1000, 500, false);

      // replace2 lifespan and max idle (test lifespan)
      c.put("k", "v1");
      f = c.replaceAsync("k", "v1", "v2", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, true);
      verifyEviction("k", "v2", 3000, 500, true);
   }

   public void testMergeAsyncWithLifespan() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.mergeAsync("k", "v1", (oldValue, newValue) -> "" + oldValue + newValue, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "vv1");
      verifyEviction("k", "vv1", 1000, 500, true);

      f = c.mergeAsync("k2", "42", (oldValue, newValue) -> "" + oldValue + newValue, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "42");
      verifyEviction("k2", "42", 1000, 500, true);
   }

   public void testMergeAsyncWithLifespanAndMaxIdle() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.mergeAsync("k", "v1", (oldValue, newValue) -> "" + oldValue + newValue, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "vv1");
      verifyEviction("k", "vv1", 1000, 500, false);

      c.put("k", "v");
      f = c.mergeAsync("k", "v1", (oldValue, newValue) -> "" + oldValue + newValue, 500, TimeUnit.MILLISECONDS, 5000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "vv1");
      verifyEviction("k", "vv1", 500, 500, false);

      f = c.mergeAsync("k2", "v", (oldValue, newValue) -> "" + oldValue + newValue, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v");
      verifyEviction("k2", "v", 1000, 500, false);

      f = c.mergeAsync("k2", "v", (oldValue, newValue) -> "" + oldValue + newValue, 500, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "v");
      verifyEviction("k2", "v", 500, 500, false);
   }

   public void testComputeAsyncWithLifespan() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.computeAsync("k", (key, value) -> "" + key + value, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 1000, 500, true);

      f = c.computeAsync("k2", (key, value) -> "" + 42, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "42");
      verifyEviction("k2", "42", 1000, 500, true);
   }

   public void testComputeAsyncWithLifespanAndMaxIdle() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.computeAsync("k", (key, value) -> "" + key + value, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 1000, 500, false);

      c.put("k", "v");
      f = c.computeAsync("k", (key, value) -> "" + key + value, 500, TimeUnit.MILLISECONDS, 5000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 500, 500, false);

      f = c.computeAsync("k2", (key, value) -> "" + 42, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "42");
      verifyEviction("k2", "42", 1000, 500, false);

      f = c.computeAsync("k2", (key, value) -> "" + value + 42, 500, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "null42");
      verifyEviction("k2", "null42", 500, 500, false);
   }

   public void testComputeIfPresentAsyncWithLifespan() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.computeIfPresentAsync("k", (key, value) -> "" + key + value, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 1000, 500, true);
   }

   public void testComputeIfPresentAsyncWithLifespanAndMaxIdle() throws Exception {
      c.put("k", "v");
      CompletableFuture<String> f = c.computeIfPresentAsync("k", (key, value) -> "" + key + value, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 1000, 500, false);

      c.put("k", "v");
      f = c.computeIfPresentAsync("k", (key, value) -> "" + key + value, 500, TimeUnit.MILLISECONDS, 5000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "kv");
      verifyEviction("k", "kv", 500, 500, false);
   }

   public void testComputeIfAbsentAsyncWithLifespan() throws Exception {
      CompletableFuture<String> f = c.computeIfAbsentAsync("k2", key -> key + 42, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "k242");
      verifyEviction("k2", "k242", 1000, 500, true);
   }

   public void testComputeIfAbsentAsyncWithLifespanAndMaxIdle() throws Exception {
      CompletableFuture<String> f = c.computeIfAbsentAsync("k2", key -> key + 42, 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "k242");
      verifyEviction("k2", "k242", 1000, 500, false);

      f = c.computeIfAbsentAsync("k2", key -> "" + key + 42, 500, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assertFutureResult(f, "k242");
      verifyEviction("k2", "k242", 500, 500, false);
   }

   /**
    * Verifies the common assertions for the obtained Future object
    *
    * @param f,        the future
    * @param expected, expected result after get
    * @throws Exception
    */
   private void assertFutureResult(Future<?> f, Object expected) throws Exception {
      assertNotNull(f);
      assertFalse(f.isCancelled());
      assertEquals(expected, f.get());
      assertTrue(f.isDone());
   }

   private void markStartTime() {
      startTime = timeService.wallClockTime();
   }

   /**
    * Verifies if a key is evicted after a certain time.
    *
    * @param key              the key to check
    * @param expectedValue    expected key value at the beginning
    * @param expectedLifetime expected life of the key
    * @param checkPeriod      period between executing checks. If the check modifies the idle time. this is important to
    *                         block idle expiration.
    * @param touchKey         indicates if the poll for key existence should read the key and cause idle time to be
    *                         reset
    */
   private void verifyEviction(final String key, final String expectedValue, final long expectedLifetime, long checkPeriod, final boolean touchKey) {
      if (startTime == null) {
         throw new IllegalStateException("markStartTime() must be called before verifyEviction(..)");
      }

      try {
         long expectedEndTime = startTime + expectedLifetime;
         Condition condition = () -> {
            if (touchKey) {
               return !c.containsKey(key);        //this check DOES read the key so it resets the idle time
            } else {
               //this check DOES NOT read the key so it does not interfere with idle time
               InternalCacheEntry entry = c.getAdvancedCache().getDataContainer().peek(key);
               return entry == null || entry.isExpired(timeService.wallClockTime());
            }
         };
         assertTrue(expectedValue.equals(c.get(key)) || timeService.wallClockTime() > expectedEndTime);
         // we need to loop to keep touching the entry and protect against idle expiration
         while (timeService.wallClockTime() <= expectedEndTime) {
            assertFalse("Entry evicted too soon!", condition.isSatisfied());
            timeService.advance(checkPeriod);
         }
         assertTrue(timeService.wallClockTime() > expectedEndTime);
         assertTrue(condition.isSatisfied());
         Object value = c.get(key);
         assertNull(value);

      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         startTime = null;
      }
   }
}
