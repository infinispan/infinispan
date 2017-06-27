package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusteredListenerJoinsTest")
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC })
public class ClusteredListenerJoinsTest extends MultipleCacheManagersTest {

   @Listener(clustered = true)
   private static final class NoOpListener {
      @CacheEntryCreated
      public void handleEvent(CacheEntryEvent<Object, Object> e) {

      }
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = buildConfiguration();
      createCluster(c, 3);
      waitForClusterToForm();
   }

   private ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, false); //unreproducible with CacheMode.REPL_SYNC
      return c;
   }

   public void testJoins() {
      int cache0Size = cache(0).getListeners().size();
      int cache1Size = cache(1).getListeners().size();

      cache(0).addListener(new NoOpListener());

      addClusterEnabledCacheManager(buildConfiguration());

      cache(0).addListener(new NoOpListener());

      waitForClusterToForm();

      // Now we verify the listener was actually added - since this is a DIST cache we also have the local listener
      // that sends remote and we added 2 of them
      assertEquals(cache0Size + 2, cache(0).getListeners().size());
      assertEquals(cache1Size + 2, cache(1).getListeners().size());
   }
}
