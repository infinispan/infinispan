package org.horizon.expiry;

import org.horizon.Cache;
import org.horizon.container.DataContainer;
import org.horizon.loader.StoredEntry;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "expiry.ExpiryTest")
public class ExpiryTest {

   CacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = new DefaultCacheManager();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testExpiryInPut() throws InterruptedException {
      Cache cache = cm.getCache();
      long lifespan = 1000;
      cache.put("k", "v", lifespan, TimeUnit.MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      StoredEntry se = dc.createEntryForStorage("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == lifespan;
      assert !se.isExpired();
      assert cache.get("k").equals("v");
      Thread.sleep(1100);
      assert se.isExpired();
      assert cache.get("k") == null;
   }

   public void testExpiryInPutAll() throws InterruptedException {
      Cache cache = cm.getCache();
      long startTime = System.currentTimeMillis();
      long lifespan = 1000;
      Map m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, lifespan, TimeUnit.MILLISECONDS);
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

   public void testExpiryInPutIfAbsent() throws InterruptedException {
      Cache cache = cm.getCache();
      long startTime = System.currentTimeMillis();
      long lifespan = 1000;
      assert cache.putIfAbsent("k", "v", lifespan, TimeUnit.MILLISECONDS) == null;
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
      assert cache.putIfAbsent("k", "v", lifespan, TimeUnit.MILLISECONDS) != null;
   }

   public void testExpiryInReplace() throws InterruptedException {
      Cache cache = cm.getCache();
      long lifespan = 1000;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", lifespan, TimeUnit.MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      long startTime = System.currentTimeMillis();
      assert cache.replace("k", "v", lifespan, TimeUnit.MILLISECONDS) != null;
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
      assert cache.replace("k", "v", "v2", lifespan, TimeUnit.MILLISECONDS);
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
}
