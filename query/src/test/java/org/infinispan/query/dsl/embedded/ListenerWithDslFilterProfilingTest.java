package org.infinispan.query.dsl.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.ListenerWithDslFilterProfilingTest")
public class ListenerWithDslFilterProfilingTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public void testEventFilterPerformance() {
      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(31)
            .toBuilder().build();

      final int numEntries = 100000;
      final int numListeners = 1000;
      List<NoOpEntryListener> listeners = new ArrayList<>(numListeners);
      for (int i = 0; i < numListeners; i++) {
         NoOpEntryListener listener = new NoOpEntryListener();
         listeners.add(listener);
         cache().addListener(listener, Search.makeFilter(query), null);
      }

      long startTs = System.nanoTime();
      for (int i = 0; i < numEntries; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache.put(i, value);
      }
      long endTs = System.nanoTime();

      for (NoOpEntryListener listener : listeners) {
         cache().removeListener(listener);
      }

      log.infof("ListenerWithDslFilterProfilingTest.testEventFilterPerformance took %d ms\n", (endTs - startTs) / 1000000);
   }

   @Listener
   public static class NoOpEntryListener {

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ?> event) {
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ?> event) {
      }
   }
}
