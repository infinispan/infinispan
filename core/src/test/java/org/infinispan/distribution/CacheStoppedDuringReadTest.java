package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.CacheStoppedDuringReadTest")
public class CacheStoppedDuringReadTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, TestDataSCI.INSTANCE, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   public void test() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));
      cache(2).put(key, "value");

      CyclicBarrier barrier0 = new CyclicBarrier(2);
      cache(0).getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(
            new BlockingInterceptor<>(barrier0, GetCacheEntryCommand.class, false, false),
            EntryWrappingInterceptor.class);

      Future<Object> f = fork(() -> cache(2).get(key));
      barrier0.await(10, TimeUnit.SECONDS);

      cache(0).stop();
      barrier0.await(10, TimeUnit.SECONDS);

      assertEquals("value", f.get(10, TimeUnit.SECONDS));
   }
}
