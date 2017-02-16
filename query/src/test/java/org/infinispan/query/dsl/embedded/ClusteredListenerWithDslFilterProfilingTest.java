package org.infinispan.query.dsl.embedded;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.ClusteredListenerWithDslFilterProfilingTest")
public class ClusteredListenerWithDslFilterProfilingTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 10;

   private final int NUM_OWNERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfgBuilder.clustering().hash().numOwners(NUM_OWNERS);
      createClusteredCaches(NUM_NODES, cfgBuilder);
   }

   public void testEventFilterPerformance() {
      long t1 = testEventFilterPerformance(false);
      long t2 = testEventFilterPerformance(true);
      log.infof("ClusteredListenerWithDslFilterProfilingTest.testEventFilterPerformance doRegisterListener=false took %d ms\n", t1 / 1000000);
      log.infof("ClusteredListenerWithDslFilterProfilingTest.testEventFilterPerformance doRegisterListener=true  took %d ms\n", t2 / 1000000);
   }

   private long testEventFilterPerformance(boolean doRegisterListener) {
      final int numEntries = 100000;
      final int numListeners = 1000;
      List<NoOpEntryListener> listeners = new ArrayList<NoOpEntryListener>(numListeners);
      if (doRegisterListener) {
         Query query = makeQuery(cache(0));
         for (int i = 0; i < numListeners; i++) {
            NoOpEntryListener listener = new NoOpEntryListener();
            listeners.add(listener);
            cache(0).addListener(listener, Search.makeFilter(query), null);
         }
      }

      long startTs = System.nanoTime();
      // create entries
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      // update entries (with same value)
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         cache.put(value.getName(), value);
      }
      long endTs = System.nanoTime();

      for (NoOpEntryListener listener : listeners) {
         cache(0).removeListener(listener);
      }

      return endTs - startTs;
   }

   private Query makeQuery(Cache c) {
      QueryFactory qf = Search.getQueryFactory(c);
      return qf.from(Person.class)
            .having("age").gte(18)
            .build();
   }

   @Listener(clustered = true)
   private static class NoOpEntryListener {

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ?> event) {
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ?> event) {
      }
   }
}
