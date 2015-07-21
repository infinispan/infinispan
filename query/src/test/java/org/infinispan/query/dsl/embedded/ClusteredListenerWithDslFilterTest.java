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
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ClusteredListenerWithDslFilterTest")
public class ClusteredListenerWithDslFilterTest extends MultipleCacheManagersTest {

   private final int NUM_NODES = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(NUM_NODES, cfgBuilder);
   }

   public void testEventFilter() {
      QueryFactory qf = Search.getQueryFactory(cache(0));

      Query query = qf.from(org.infinispan.query.test.Person.class)
            .having("age").lte(31)
            .toBuilder().build();

      EntryListener listener = new EntryListener();

      for (int i = 0; i < 5; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 30);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, value);
      }

      // we want our cluster listener to be notified only if the entity matches our query
      cache(0).addListener(listener, Search.makeFilter(query), null);

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, value);
      }

      assertEquals(9, listener.results.size());
      for (ObjectFilter.FilterResult r : listener.results) {
         Person p = (Person) r.getInstance();
         assertTrue(p.getAge() <= 31);
      }

      cache(0).removeListener(listener);

      // ensure no more invocations after the listener was removed
      listener.results.clear();
      Person value = new Person();
      value.setName("George");
      value.setAge(30);

      Object key = new MagicKey(cache(0));
      cache(0).put(key, value);
      assertEquals(0, listener.results.size());
   }

   public void testEventFilterAndConverter() {
      QueryFactory qf = Search.getQueryFactory(cache(0));

      Query query = qf.from(org.infinispan.query.test.Person.class)
            .having("age").lte(31)
            .toBuilder()
            .select("name", "age")
            .build();

      EntryListener listener = new EntryListener();

      for (int i = 0; i < 5; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 30);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, value);
      }

      // we want our cluster listener to be notified only if the entity matches our query
      cache(0).addListener(listener, Search.makeFilter(query), null);

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         Cache<Object, Person> cache = cache(i % NUM_NODES);
         Object key = new MagicKey(cache);
         cache.put(key, value);
      }

      assertEquals(9, listener.results.size());
      for (ObjectFilter.FilterResult r : listener.results) {
         assertTrue((Integer) r.getProjection()[1] <= 31);
      }

      cache(0).removeListener(listener);
   }

   @Listener(clustered = true, includeCurrentState = true)
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
