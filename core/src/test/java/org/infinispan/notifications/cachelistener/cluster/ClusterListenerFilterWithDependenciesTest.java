package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerFilterWithDependenciesTest")
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC })
public class ClusterListenerFilterWithDependenciesTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 2;

   private final int NUM_ENTRIES = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(cacheMode, false);
      createClusteredCaches(NUM_NODES, cfgBuilder);
   }

   public void testEventFilterCurrentState() {
      for (int i = 0; i < NUM_ENTRIES; ++i) {
         Cache<Object, String> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, "string " + i);
      }
      assertEquals(NUM_ENTRIES, cache(0).size());

      EntryListener listener = new EntryListener();
      NoOpCacheEventFilterConverterWithDependencies filterConverter = new NoOpCacheEventFilterConverterWithDependencies();
      cache(0).addListener(listener, filterConverter, filterConverter);

      assertEquals(NUM_ENTRIES, listener.createEvents.size());

      cache(0).removeListener(listener);
   }

   public void testEventFilter() {
      EntryListener listener = new EntryListener();
      NoOpCacheEventFilterConverterWithDependencies filterConverter = new NoOpCacheEventFilterConverterWithDependencies();
      cache(0).addListener(listener, filterConverter, filterConverter);

      for (int i = 0; i < NUM_ENTRIES; ++i) {
         Cache<Object, String> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, "string " + i);
      }

      assertEquals(NUM_ENTRIES, cache(0).size());
      assertEquals(NUM_ENTRIES, listener.createEvents.size());

      cache(0).removeListener(listener);
   }

   @Listener(clustered = true, includeCurrentState = true)
   public static class EntryListener {

      public final List<CacheEntryCreatedEvent> createEvents = new ArrayList<CacheEntryCreatedEvent>();

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent event) {
         if (!event.isPre()) {
            createEvents.add(event);
         }
      }
   }
}
