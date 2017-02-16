package org.infinispan.query.dsl.embedded;

import static org.infinispan.query.dsl.Expression.max;
import static org.infinispan.query.dsl.Expression.param;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ListenerWithDslFilterTest")
public class ListenerWithDslFilterTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getConfigurationBuilder());
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfgBuilder;
   }

   public void testEventFilter() {
      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(99);

         cache().put(i, value);
      }
      assertEquals(10, cache.size());

      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(param("ageParam"))
            .build().setParameter("ageParam", 31);

      EntryListener listener = new EntryListener();

      // we want our cluster listener to be notified only if the entity matches our query
      cache().addListener(listener, Search.makeFilter(query), null);

      assertTrue(listener.createEvents.isEmpty());
      assertTrue(listener.modifyEvents.isEmpty());

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache().put(i, value);
      }

      assertEquals(10, cache.size());
      assertTrue(listener.createEvents.isEmpty());
      assertEquals(7, listener.modifyEvents.size());

      for (ObjectFilter.FilterResult r : listener.modifyEvents) {
         Person p = (Person) r.getInstance();
         assertTrue(p.getAge() <= 31);
      }

      cache().removeListener(listener);

      // ensure no more invocations after the listener was removed
      listener.createEvents.clear();
      listener.modifyEvents.clear();
      Person value = new Person();
      value.setName("George");
      value.setAge(30);

      cache().put(-1, value);
      assertTrue(listener.createEvents.isEmpty());
      assertTrue(listener.modifyEvents.isEmpty());
   }

   public void testEventFilterChangingParameter() {
      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(99);

         cache().put(i, value);
      }
      assertEquals(10, cache.size());

      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(param("ageParam"))
            .build().setParameter("ageParam", 31);

      EntryListener listener = new EntryListener();

      // we want our cluster listener to be notified only if the entity matches our query
      cache().addListener(listener, Search.makeFilter(query), null);

      assertTrue(listener.createEvents.isEmpty());
      assertTrue(listener.modifyEvents.isEmpty());

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache().put(i, value);
      }

      assertEquals(10, cache.size());
      assertTrue(listener.createEvents.isEmpty());
      assertEquals(7, listener.modifyEvents.size());

      for (ObjectFilter.FilterResult r : listener.modifyEvents) {
         Person p = (Person) r.getInstance();
         assertTrue(p.getAge() <= 31);
      }

      cache().removeListener(listener);

      query.setParameter("ageParam", 30);

      listener = new EntryListener();

      cache().addListener(listener, Search.makeFilter(query), null);

      assertTrue(listener.createEvents.isEmpty());
      assertTrue(listener.modifyEvents.isEmpty());

      for (int i = 0; i < 10; ++i) {
         Person value = new Person();
         value.setName("John");
         value.setAge(i + 25);

         cache().put(i, value);
      }

      assertEquals(10, cache.size());
      assertTrue(listener.createEvents.isEmpty());
      assertEquals(6, listener.modifyEvents.size());

      for (ObjectFilter.FilterResult r : listener.modifyEvents) {
         Person p = (Person) r.getInstance();
         assertTrue(p.getAge() <= 30);
      }
   }

   public void testEventFilterAndConverter() {
      QueryFactory qf = Search.getQueryFactory(cache());

      Query query = qf.from(Person.class)
            .having("age").lte(31)
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

      assertEquals(10, cache.size());
      assertEquals(7, listener.createEvents.size());
      assertTrue(listener.modifyEvents.isEmpty());

      for (ObjectFilter.FilterResult r : listener.createEvents) {
         assertNotNull(r.getProjection());
         assertEquals(2, r.getProjection().length);
         assertTrue((Integer) r.getProjection()[1] <= 31);
      }

      cache().removeListener(listener);
   }

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028523: Filters cannot use full-text searches")
   public void testDisallowFullTextQuery() {
      Query query = Search.getQueryFactory(cache()).create("from org.infinispan.query.test.Person where name : 'john'");

      cache().addListener(new EntryListener(), Search.makeFilter(query), null);
   }

   /**
    * Using grouping and aggregation with event filters is not allowed.
    */
   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*ISPN028509:.*")
   public void testDisallowGroupingAndAggregation() {
      Query query = Search.getQueryFactory(cache()).from(Person.class)
            .having("age").gte(20)
            .select(max("age"))
            .build();

      cache().addListener(new EntryListener(), Search.makeFilter(query), null);
   }

   @Listener(observation = Listener.Observation.POST)
   private static class EntryListener {

      // this is where we accumulate matches
      public final List<ObjectFilter.FilterResult> createEvents = new ArrayList<>();

      public final List<ObjectFilter.FilterResult> modifyEvents = new ArrayList<>();

      @CacheEntryCreated
      public void handleEvent(CacheEntryCreatedEvent<?, ObjectFilter.FilterResult> event) {
         ObjectFilter.FilterResult filterResult = event.getValue();
         createEvents.add(filterResult);
      }

      @CacheEntryModified
      public void handleEvent(CacheEntryModifiedEvent<?, ObjectFilter.FilterResult> event) {
         ObjectFilter.FilterResult filterResult = event.getValue();
         modifyEvents.add(filterResult);
      }
   }
}
