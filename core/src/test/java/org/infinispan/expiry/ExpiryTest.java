package org.infinispan.expiry;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Test(groups = "functional", testName = "expiry.ExpiryTest")
public class ExpiryTest {

   CacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testLifespanExpiryInPut() throws InterruptedException {
      Cache cache = cm.getCache();
      long lifespan = 1000;
      cache.put("k", "v", lifespan, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == lifespan;
      assert se.getMaxIdle() == -1;
      assert !se.isExpired();
      assert cache.get("k").equals("v");
      Thread.sleep(1100);
      assert se.isExpired();
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInPut() throws InterruptedException {
      Cache cache = cm.getCache();
      long idleTime = 1000;
      cache.put("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == -1;
      assert se.getMaxIdle() == idleTime;
      assert !se.isExpired();
      assert cache.get("k").equals("v");
      Thread.sleep(1100);
      assert se.isExpired();
      assert cache.get("k") == null;
   }

   public void testLifespanExpiryInPutAll() throws InterruptedException {
      Cache cache = cm.getCache();
      long startTime = System.currentTimeMillis();
      long lifespan = 1000;
      Map m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, lifespan, MILLISECONDS);
      while (System.currentTimeMillis() < startTime + lifespan) {
         assert cache.get("k1").equals("v");
         assert cache.get("k2").equals("v");
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k1") == null && cache.get("k2") == null) return;
      }
      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testIdleExpiryInPutAll() throws InterruptedException {
      Cache cache = cm.getCache();
      long idleTime = 10000;
      Map m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, -1, MILLISECONDS, idleTime, MILLISECONDS);
      assert cache.get("k1").equals("v");
      assert cache.get("k2").equals("v");

      Thread.sleep(idleTime + 100);

      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testLifespanExpiryInPutIfAbsent() throws InterruptedException {
      Cache cache = cm.getCache();
      long startTime = System.currentTimeMillis();
      long lifespan = 1000;
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) == null;
      while (System.currentTimeMillis() < startTime + lifespan) {
         assert cache.get("k").equals("v");
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) != null;
   }

   public void testIdleExpiryInPutIfAbsent() throws InterruptedException {
      Cache cache = cm.getCache();
      long idleTime = 10000;
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert cache.get("k").equals("v");

      Thread.sleep(idleTime + 100);

      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
   }

   public void testLifespanExpiryInReplace() throws InterruptedException {
      Cache cache = cm.getCache();
      long lifespan = 10000;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", lifespan, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      long startTime = System.currentTimeMillis();
      assert cache.replace("k", "v", lifespan, MILLISECONDS) != null;
      assert cache.get("k").equals("v");
      while (System.currentTimeMillis() < startTime + lifespan) {
         assert cache.get("k").equals("v");
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;


      startTime = System.currentTimeMillis();
      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", lifespan, MILLISECONDS);
      while (System.currentTimeMillis() < startTime + lifespan) {
         assert cache.get("k").equals("v2");
         Thread.sleep(50);
      }

      //make sure that in the next 2 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInReplace() throws InterruptedException {
      Cache cache = cm.getCache();
      long idleTime = 10000;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
      assert cache.get("k").equals("v");

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", -1, MILLISECONDS, idleTime, MILLISECONDS);

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;
   }
}
