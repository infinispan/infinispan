package org.infinispan.query.backend;

import org.apache.lucene.index.SegmentInfos;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.jdk8backported.LongAdder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.query.test.Person;
import org.infinispan.test.CacheManagerCallable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

import static org.infinispan.test.TestingUtil.recursiveFileRemove;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.*;

/**
 * Test for interaction of activation and preload on indexing
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.QueryInterceptorTest")
public class QueryInterceptorTest {

   private static final int MAX_CACHE_ENTRIES = 1;
   private File indexDir;
   private File storeDir;

   private final Person person1 = new Person("p1", "b1", 12);
   private final Person person2 = new Person("p2", "b2", 22);

   @BeforeMethod
   protected void setup() throws Exception {
      indexDir = Files.createTempDirectory("test-").toFile();
      storeDir = Files.createTempDirectory("test-").toFile();
   }

   @AfterMethod
   protected void tearDown() {
      recursiveFileRemove(indexDir);
      recursiveFileRemove(storeDir);
   }

   @Test
   public void shouldNotReindexOnActivation() throws Exception {
      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES)) {
         @Override
         public void call() {
            LuceneIndexTracker luceneIndexTracker = new LuceneIndexTracker(new File(indexDir + "/person"));
            luceneIndexTracker.mark();

            Cache<Integer, Person> cache = cm.getCache();
            CacheListener cacheListener = new CacheListener();
            cache.addListener(cacheListener);

            cache.put(1, person1);
            cache.put(2, person2);

            assertEquals(cacheListener.numberOfPassivations(), 1);
            assertEquals(cacheListener.numberOfActivations(), 0);
            assertTrue(luceneIndexTracker.indexChanged());

            luceneIndexTracker.mark();

            cache.get(1);

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
            Cache<Integer, Person> cache = cm.getCache();
            cache.put(1, person1);
            cache.put(2, person2);
            assertTrue(luceneIndexTracker.indexChanged());
         }
      });

      luceneIndexTracker.mark();

      withCacheManager(new CacheManagerCallable(createCacheManager(MAX_CACHE_ENTRIES + 10)) {
         @Override
         public void call() {
            Cache<Integer, Person> cache = cm.getCache();
            CacheListener cacheListener = new CacheListener();
            cache.addListener(cacheListener);

            assertTrue(cache.containsKey(1));
            assertTrue(cache.containsKey(2));
            assertEquals(cacheListener.numberOfPassivations(), 0);
            assertEquals(cacheListener.numberOfActivations(), 0);
            assertFalse(luceneIndexTracker.indexChanged());
         }
      });

   }

   protected EmbeddedCacheManager createCacheManager(int maxEntries) throws Exception {
      return new DefaultCacheManager(
            new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build(),
            new ConfigurationBuilder()
                  .eviction().strategy(EvictionStrategy.LRU).maxEntries(maxEntries)
                  .persistence().passivation(true)
                  .addSingleFileStore().location(storeDir.getAbsolutePath()).preload(true)
                  .indexing().index(Index.ALL)
                  .addProperty("default.directory_provider", "filesystem")
                  .addProperty("default.indexBase", indexDir.getAbsolutePath())
                  .addProperty("lucene_version", "LUCENE_CURRENT")
                  .build()
      );
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
         return SegmentInfos.getLastCommitGeneration(indexBase.list());
      }
   }

   @Listener
   private static final class CacheListener {

      private final LongAdder passivationCount = new LongAdder();
      private final LongAdder activationCount = new LongAdder();

      @CacheEntryPassivated
      public void onEvent(CacheEntryPassivatedEvent payload) {
         if (!payload.isPre())
            passivationCount.increment();
      }

      @CacheEntryActivated
      public void onEvent(CacheEntryActivatedEvent payload) {
         if (!payload.isPre())
            activationCount.increment();
      }

      public int numberOfPassivations() {
         return passivationCount.intValue();
      }

      public int numberOfActivations() {
         return activationCount.intValue();
      }
   }
}
