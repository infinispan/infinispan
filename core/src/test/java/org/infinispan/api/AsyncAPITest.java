package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.AsyncAPITest")
public class AsyncAPITest extends SingleCacheManagerTest {

   private Cache<String, String> c;
   private ControlledTimeService timeService = new ControlledTimeService();
   private Long startTime;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      c = cm.getCache();
      return cm;
   }

   public void testAsyncMethods() throws Exception {
      // get
      Future<String> f = c.getAsync("k");
      assert f != null;
      assert !f.isCancelled();
      assertNull(f.get());
      assert f.isDone();
      assert c.get("k") == null;

      // put
      f = c.putAsync("k", "v");
      assert f != null;
      assert !f.isCancelled();
      assertEquals(f.get(), null);
      assert f.isDone();
      assert c.get("k").equals("v");

      f = c.putAsync("k", "v2");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert f.isDone();
      assert c.get("k").equals("v2");

      // putAll
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"));
      assert f2 != null;
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      assert c.get("k").equals("v3");

      // putIfAbsent
      f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert f.isDone();
      assert c.get("k").equals("v3");

      // remove
      f = c.removeAsync("k");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v3");
      assert f.isDone();
      assert c.get("k") == null;

      // putIfAbsent again
      f = c.putIfAbsentAsync("k", "v4");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assert c.get("k").equals("v4");

      // get
      f = c.getAsync("k");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v4");
      assert f.isDone();
      assert c.get("k").equals("v4");

      // removecond
      Future<Boolean> f3 = c.removeAsync("k", "v_nonexistent");
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assert c.get("k").equals("v4");

      f3 = c.removeAsync("k", "v4");
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assert c.get("k") == null;

      // replace
      f = c.replaceAsync("k", "v5");
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assert c.get("k") == null;

      // putAllAsync
      Map<String, String> map = new HashMap<>();
      map.put("k", "v");
      map.put("other-key", "other-value");
      CompletableFuture<Void> putAllF = c.putAllAsync(map);
      assertNotNull(putAllF);
      assertFalse(putAllF.isCancelled());
      assertEquals(null, putAllF.get());
      assertTrue(putAllF.isDone());
      assertEquals("v", c.get("k"));
      assertEquals("other-value", c.get("other-key"));

      // getAllAsync
      CompletableFuture<Map<String, String>> getAllF = c.getAllAsync(map.keySet());
      assertNotNull(getAllF);
      assertFalse(getAllF.isCancelled());
      assertEquals(map, getAllF.get());
      assertTrue(getAllF.isDone());

      // replace2
      f = c.replaceAsync("k", "v5");
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert f.isDone();
      assert c.get("k").equals("v5");

      // replace3
      f3 = c.replaceAsync("k", "v_nonexistent", "v6");
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      assert c.get("k").equals("v5");

      f3 = c.replaceAsync("k", "v5", "v6");
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      assert c.get("k").equals("v6");
   }

   public void testAsyncMethodWithLifespanAndMaxIdle() throws Exception {

      // lifespan only
      Future<String> f = c.putAsync("k", "v", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 1000, 500, true);

      log.warn("STARTING FAILING ONE");

      // lifespan and max idle (test max idle)
      f = c.putAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 1000, 500, false);

      // lifespan and max idle (test lifespan)
      f = c.putAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 3000, 500, true);

      // putAll lifespan only
      Future<Void> f2 = c.putAllAsync(Collections.singletonMap("k", "v3"), 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f2 != null;
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      verifyEviction("k", "v3", 1000, 500, true);

      // putAll lifespan and max idle (test max idle)
      f2 = c.putAllAsync(Collections.singletonMap("k", "v4"), 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f2 != null;
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      verifyEviction("k", "v4", 1000, 500, false);

      // putAll lifespan and max idle (test lifespan)
      f2 = c.putAllAsync(Collections.singletonMap("k", "v5"), 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f2 != null;
      assert !f2.isCancelled();
      assert f2.get() == null;
      assert f2.isDone();
      verifyEviction("k", "v5", 3000, 500, true);

      // putIfAbsent lifespan only
      f = c.putAsync("k", "v3");
      assertNull(f.get());
      f = c.putIfAbsentAsync("k", "v4", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assertEquals("v3", f.get());
      assert f.isDone();
      assert c.get("k").equals("v3");
      assert !c.get("k").equals("v4");
      Thread.sleep(300);
      assert c.get("k").equals("v3");
      f = c.removeAsync("k");
      assert f.get().equals("v3");
      assert c.get("k") == null;

      // now really put (k removed) lifespan only
      f = c.putIfAbsentAsync("k", "v", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 1000, 500, true);

      // putIfAbsent lifespan and max idle (test max idle)
      f = c.putIfAbsentAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 1000, 500, false);

      // putIfAbsent lifespan and max idle (test lifespan)
      f = c.putIfAbsentAsync("k", "v", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      verifyEviction("k", "v", 3000, 500, true);

      // replace
      f = c.replaceAsync("k", "v5", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get() == null;
      assert f.isDone();
      assert c.get("k") == null;

      // replace lifespan only
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert f.isDone();
      verifyEviction("k", "v5", 1000, 500, true);

      // replace lifespan and max idle (test max idle)
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert f.isDone();
      verifyEviction("k", "v5", 1000, 500, false);

      // replace lifespan and max idle (test lifespan)
      c.put("k", "v");
      f = c.replaceAsync("k", "v5", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f != null;
      assert !f.isCancelled();
      assert f.get().equals("v");
      assert f.isDone();
      verifyEviction("k", "v5", 3000, 500, true);

      //replace2
      c.put("k", "v5");
      Future<Boolean> f3 = c.replaceAsync("k", "v_nonexistent", "v6", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(false);
      assert f3.isDone();
      Thread.sleep(300);
      assert c.get("k").equals("v5");

      // replace2 lifespan only
      f3 = c.replaceAsync("k", "v5", "v6", 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      verifyEviction("k", "v6", 1000, 500, true);

      // replace2 lifespan and max idle (test max idle)
      c.put("k", "v5");
      f3 = c.replaceAsync("k", "v5", "v6", 5000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      verifyEviction("k", "v6", 1000, 500, false);

      // replace2 lifespan and max idle (test lifespan)
      c.put("k", "v5");
      f3 = c.replaceAsync("k", "v5", "v6", 3000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
      markStartTime();
      assert f3 != null;
      assert !f3.isCancelled();
      assert f3.get().equals(true);
      assert f3.isDone();
      verifyEviction("k", "v6", 3000, 500, true);
   }

   private void markStartTime() {
      startTime = timeService.wallClockTime();
   }

   /**
    * Verifies if a key is evicted after a certain time.
    *  @param key the key to check
    * @param expectedValue expected key value at the beginning
    * @param expectedLifetime expected life of the key
    * @param checkPeriod period between executing checks. If the check modifies the idle time. this is important to block idle expiration.
    * @param touchKey indicates if the poll for key existence should read the key and cause idle time to be reset
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

   private static class ControlledTimeService extends DefaultTimeService {
      private long time = super.wallClockTime();

      @Override
      public long wallClockTime() {
         return time;
      }

      @Override
      public long time() {
         return TimeUnit.MILLISECONDS.toNanos(time);
      }

      public void advance(long millis) {
         time += millis;
      }
   }
}
