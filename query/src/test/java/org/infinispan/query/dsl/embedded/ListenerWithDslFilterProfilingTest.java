package org.infinispan.query.dsl.embedded;

import java.util.ArrayList;
import java.util.List;

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
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "profiling", testName = "query.dsl.embedded.ListenerWithDslFilterProfilingTest")
public class ListenerWithDslFilterProfilingTest extends SingleCacheManagerTest {

   private static final int NUM_ENTRIES = 100000;
   private static final int NUM_LISTENERS = 1000;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE);
   }

   public void testEventFilterPerformance() {
      QueryFactory qf = Search.getQueryFactory(cache());

      Query<Person> query = qf.create("FROM org.infinispan.query.test.Person WHERE age <= 31");

      List<NoOpEntryListener> listeners = new ArrayList<>(NUM_LISTENERS);
      for (int i = 0; i < NUM_LISTENERS; i++) {
         NoOpEntryListener listener = new NoOpEntryListener();
         listeners.add(listener);
         cache().addListener(listener, Search.makeFilter(query), null);
      }

      long startTs = System.nanoTime();
      for (int i = 0; i < NUM_ENTRIES; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache.put(i, value);
      }
      long endTs = System.nanoTime();

      for (NoOpEntryListener listener : listeners) {
         cache().removeListener(listener);
      }

      log.infof("ListenerWithDslFilterProfilingTest.testEventFilterPerformance took %d us\n", (endTs - startTs) / 1000);
   }

   @Listener
   private static class NoOpEntryListener {

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ?> event) {
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ?> event) {
      }
   }
}
