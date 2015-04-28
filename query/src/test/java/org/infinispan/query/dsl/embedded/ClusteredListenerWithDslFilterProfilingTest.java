package org.infinispan.query.dsl.embedded;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.ClusteredListenerWithDslFilterProfilingTest")
public class ClusteredListenerWithDslFilterProfilingTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(NUM_NODES, cfgBuilder);
   }

   public void testEventFilterPerformance() {
      QueryFactory qf = Search.getQueryFactory(cache(0));

      Query query = qf.from(Person.class)
            .having("age").lte(31)
            .toBuilder().build();

      final int numEntries = 100000;
      final int numListeners = 1000;
      List<NoOpEntryListener> listeners = new ArrayList<>(numListeners);
      for (int i = 0; i < numListeners; i++) {
         NoOpEntryListener listener = new NoOpEntryListener();
         listeners.add(listener);
         cache(0).addListener(listener, Search.makeFilter(query), null);
      }

      long startTs = System.nanoTime();
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, value);
      }
      long endTs = System.nanoTime();

      for (NoOpEntryListener listener : listeners) {
         cache(0).removeListener(listener);
      }

      log.infof("ClusteredListenerWithDslFilterProfilingTest.testEventFilterPerformance took %d ms\n", (endTs - startTs) / 1000000);
   }

   @Listener(clustered = true)
   public static class NoOpEntryListener {

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ?> event) {
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ?> event) {
      }
   }
}
