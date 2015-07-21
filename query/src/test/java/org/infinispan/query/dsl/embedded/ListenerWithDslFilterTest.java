package org.infinispan.query.dsl.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ListenerWithDslFilterTest")
public class ListenerWithDslFilterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public void testEventFilter() {
      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(31)
            .toBuilder().build();

      EntryListener listener = new EntryListener();

      // we want our cluster listener to be notified only if the entity matches our query
      cache().addListener(listener, Search.makeFilter(query), null);

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache().put(i, value);
      }

      assertEquals(7, listener.results.size());
      for (ObjectFilter.FilterResult r : listener.results) {
         Person p = (Person) r.getInstance();
         assertTrue(p.getAge() <= 31);
      }

      cache().removeListener(listener);

      // ensure no more invocations after the listener was removed
      listener.results.clear();
      Person value = new Person();
      value.setName("George");
      value.setAge(30);

      cache().put(-1, value);
      assertEquals(0, listener.results.size());
   }

   public void testEventFilterAndConverter() {
      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(31)
            .toBuilder()
            .select("name", "age")
            .build();

      EntryListener listener = new EntryListener();

      // we want our cluster listener to be notified only if the entity matches our query
      cache().addListener(listener, Search.makeFilter(query), null);

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache.put(i, value);
      }

      assertEquals(7, listener.results.size());
      for (ObjectFilter.FilterResult r : listener.results) {
         assertTrue((Integer) r.getProjection()[1] <= 31);
      }

      cache().removeListener(listener);
   }

   @Listener
   public static class EntryListener {

      // this is where we accumulate matches
      public final List<ObjectFilter.FilterResult> results = new ArrayList<ObjectFilter.FilterResult>();

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ObjectFilter.FilterResult> event) {
         if (!event.isPre()) {
            ObjectFilter.FilterResult filterResult = event.getValue();
            results.add(filterResult);
         }
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ObjectFilter.FilterResult> event) {
         if (!event.isPre()) {
            ObjectFilter.FilterResult filterResult = event.getValue();
            results.add(filterResult);
         }
      }
   }
}
