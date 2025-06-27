package org.infinispan.query.backend;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.LongAdder;

import org.apache.lucene.index.SegmentInfos;
import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for interaction of activation and preload on indexing.
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.QueryInterceptorTest")
public class QueryInterceptorTest extends AbstractInfinispanTest {

   private static final int MAX_CACHE_ENTRIES = 1;
   private File indexDir;
   private File storeDir;

   private final Person person1 = new Person("p1", "b1", 12);
   private final Person person2 = new Person("p2", "b2", 22);
   private final Car car1 = new Car("subaru", "blue", 200);
   private final Car car2 = new Car("lamborghini", "yellow", 230);

   @BeforeMethod
   protected void setup() throws Exception {
      indexDir = Files.createTempDirectory("test-").toFile();
      storeDir = Files.createTempDirectory("test-").toFile();
   }

   @AfterMethod
   protected void tearDown() {
      Util.recursiveFileRemove(indexDir);
      Util.recursiveFileRemove(storeDir);
   }

   @Test
   public void shouldNotReindexOnActivation() throws Exception {
      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES)) {
         @Override
         public void call() {
            LuceneIndexTracker luceneIndexTracker = new LuceneIndexTracker(new File(indexDir + "/person"));
            luceneIndexTracker.mark();

            Cache<String, Person> cache = cm.getCache();
            CacheListener<String, Person> cacheListener = new CacheListener<>();
            cache.addListener(cacheListener);

            cache.put("key1", person1);
            cache.put("key2", person2);
            forceCommit(cache);

            // Notification is non blocking
            eventuallyEquals(1, cacheListener::numberOfPassivations);
            assertEquals(cacheListener.numberOfActivations(), 0);
            assertTrue(luceneIndexTracker.indexChanged());

            luceneIndexTracker.mark();

            cache.get("key1");

            assertEquals(cacheListener.numberOfActivations(), 1);
            assertFalse(luceneIndexTracker.indexChanged());
         }
      });
   }

   @Test
   public void shouldNotReindexOnPreload() throws Exception {
      final LuceneIndexTracker luceneIndexTracker = new LuceneIndexTracker(new File(indexDir + "/person"));
      luceneIndexTracker.mark();

      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES)) {
         @Override
         public void call() {
            Cache<String, Person> cache = cm.getCache();
            cache.put("key1", person1);
            cache.put("key2", person2);
            forceCommit(cache);
            assertTrue(luceneIndexTracker.indexChanged());
         }
      });

      luceneIndexTracker.mark();

      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES + 10)) {
         @Override
         public void call() {
            Cache<String, Person> cache = cm.getCache();
            CacheListener<String, Person> cacheListener = new CacheListener<>();
            cache.addListener(cacheListener);

            assertTrue(cache.containsKey("key1"));
            assertTrue(cache.containsKey("key2"));
            assertEquals(cacheListener.numberOfPassivations(), 0);
            assertEquals(cacheListener.numberOfActivations(), 0);
            assertFalse(luceneIndexTracker.indexChanged());
         }
      });
   }

   @Test
   public void shouldDeleteSingleIndex() throws Exception {
      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES)) {
         @Override
         public void call() {
            Cache<String, Object> cache = cm.getCache();
            cache.put("P1", person1);
            cache.put("P2", person2);
            cache.put("C1", car1);
            cache.put("C2", car2);
            forceCommit(cache);

            assertEquals(2, countIndex(Car.class, cache));
            assertEquals(2, countIndex(Person.class, cache));

            Indexer indexer = Search.getIndexer(cache);

            CompletionStages.join(indexer.remove(Car.class));
            assertEquals(0, countIndex(Car.class, cache));
            assertEquals(2, countIndex(Person.class, cache));

            CompletionStages.join(indexer.remove(Person.class));
            assertEquals(0, countIndex(Car.class, cache));
            assertEquals(0, countIndex(Person.class, cache));
         }
      });
   }

   protected EmbeddedCacheManager createCacheManager(int maxEntries) throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.globalState().enable().persistentLocation(storeDir.getAbsolutePath());
      globalBuilder.serialization().addContextInitializer(QueryTestSCI.INSTANCE);

      ConfigurationBuilder b = new ConfigurationBuilder();
      b.memory().maxCount(maxEntries)
            .persistence().passivation(true)
            .addSingleFileStore().preload(true)
            .indexing().enable()
            .storage(IndexStorage.FILESYSTEM).path(indexDir.getAbsolutePath())
            .addIndexedEntity(Person.class)
            .addIndexedEntity(Car.class);
      return TestCacheManagerFactory.createCacheManager(globalBuilder, b);
   }

   private long countIndex(Class<?> entityType, Cache<?, ?> cache) {
      Query<?> query = cache.query("FROM " + entityType.getName());
      return query.execute().count().value();
   }

   private void forceCommit(Cache<?, ?> cache) {
      TestQueryHelperFactory.extractSearchMapping(cache).scopeAll().workspace().flush();
   }

   private static final class LuceneIndexTracker {

      private final File indexBase;
      private long indexVersion;

      public LuceneIndexTracker(File indexBase) {
         this.indexBase = indexBase;
      }

      public void mark() {
         indexVersion = getLuceneIndexVersion();
      }

      public boolean indexChanged() {
         return getLuceneIndexVersion() != indexVersion;
      }

      private long getLuceneIndexVersion() {
         return indexBase.list() == null ? -1 : SegmentInfos.getLastCommitGeneration(indexBase.list());
      }
   }

   @Listener
   @SuppressWarnings("unused")
   private static final class CacheListener<K, V> {

      private final LongAdder passivationCount = new LongAdder();
      private final LongAdder activationCount = new LongAdder();

      @CacheEntryPassivated
      public void onEvent(CacheEntryPassivatedEvent<K, V> payload) {
         if (!payload.isPre()) {
            passivationCount.increment();
         }
      }

      @CacheEntryActivated
      public void onEvent(CacheEntryActivatedEvent<K, V> payload) {
         if (!payload.isPre()) {
            activationCount.increment();
         }
      }

      public int numberOfPassivations() {
         return passivationCount.intValue();
      }

      public int numberOfActivations() {
         return activationCount.intValue();
      }
   }
}
