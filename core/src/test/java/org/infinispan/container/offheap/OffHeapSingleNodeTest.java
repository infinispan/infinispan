package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.encoding.DataConversion;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 */
@Test(groups = "functional", testName = "container.offheap.OffHeapSingleNodeTest")
public class OffHeapSingleNodeTest extends OffHeapMultiNodeTest {

   protected ControlledTimeService timeService;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dcc.memory().storage(StorageType.OFF_HEAP);
      // Only start up the 1 cache
      addClusterEnabledCacheManager(dcc);

      configureTimeService();
   }

   protected void configureTimeService() {
      timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(cacheManagers.get(0), TimeService.class, timeService, true);
   }

   public void testLotsOfWrites() {
      Cache<String, String> cache = cache(0);

      for (int i = 0; i < 5_000; ++i) {
         cache.put("key" + i, "value" + i);
      }
   }

   public void testExpiredEntryCompute() throws IOException, InterruptedException {
      Cache<Object, Object> cache = cache(0);

      String key = "key";

      cache.put(key, "value", 10, TimeUnit.MILLISECONDS);

      timeService.advance(20);

      DataConversion keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();

      WrappedBytes keyWB = (WrappedBytes) keyDataConversion.toStorage(key);

      AtomicBoolean invoked = new AtomicBoolean(false);
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      container.compute(keyWB, (k, e, f) -> {
         invoked.set(true);
         // Just leave it in there
         return e;
      });

      // Should not have invoked, due to expiration
      assertTrue(invoked.get());

      assertNotNull(container.peek(keyWB));

      // Actually reading it shouldn't return though and actually remove
      assertNull(cache.get(key));

      // Now that we did a get, the peek shouldn't return anything
      assertNull(container.peek(keyWB));
   }
}
