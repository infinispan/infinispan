package org.infinispan.query.startup;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.model.Developer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.startup.IndexStartupModeTest")
public class IndexStartupModeTest extends AbstractInfinispanTest {

   private final String fileStoreDataLocation = CommonsTestingUtil.tmpDirectory("IndexStartupModeTest", "fileStoreDataLocation");
   private final String fileStoreIndexLocation = CommonsTestingUtil.tmpDirectory("IndexStartupModeTest", "fileStoreIndexLocation");
   private final String indexesLocation = CommonsTestingUtil.tmpDirectory("IndexStartupModeTest", "indexes");

   private EmbeddedCacheManager cacheManager;
   private Cache<String, Developer> cache;

   public void volatileDataNonVolatileIndexes_purgeAtStartup() {
      execute(IndexStorage.FILESYSTEM, false, IndexStartupMode.PURGE, () -> {
         verifyMatches(0, "fax4ever");

         cache.put("fabio", new Developer("fax4ever", "fax@redmail.io", "Infinispan developer", 0, "Infinispan developer"));

         verifyMatches(1, "fax4ever");
      });

      execute(IndexStorage.FILESYSTEM, false, IndexStartupMode.NONE, () -> {
         // data is not present anymore
         assertThat(cache.get("fabio")).isNull();

         // but by default no purge is applied on persisted indexes
         verifyMatches(1, "fax4ever");
      });

      execute(IndexStorage.FILESYSTEM, false, IndexStartupMode.PURGE, () -> {
         // with the initial purge the persisted indexes are wiped out at cache startup
         verifyMatches(0, "fax4ever");

         cache.put("fabio", new Developer("fax4ever", "fax@redmail.io", "Infinispan developer", 0, "Infinispan developer"));

         // recreate the index for the next executions
         verifyMatches(1, "fax4ever");
      });

      execute(IndexStorage.FILESYSTEM, false, IndexStartupMode.NONE, () -> {
         // data is not present anymore
         assertThat(cache.get("fabio")).isNull();

         // but by default no purge is applied on persisted indexes
         verifyMatches(1, "fax4ever");
      });

      execute(IndexStorage.FILESYSTEM, false, IndexStartupMode.AUTO, () -> {
         // auto in this case is equivalent to purge
         verifyMatches(0, "fax4ever");
      });
   }

   public void nonVolatileDataVolatileIndexes_reindexAtStartup() {
      execute(IndexStorage.LOCAL_HEAP, true, IndexStartupMode.NONE, () -> {
         verifyMatches(0, "fax4ever");

         cache.put("fabio", new Developer("fax4ever", "fax@redmail.io", "Infinispan developer", 0, "Infinispan developer"));

         verifyMatches(1, "fax4ever");
      });

      execute(IndexStorage.LOCAL_HEAP, true, IndexStartupMode.NONE, () -> {
         // data is still present
         Developer fabio = cache.get("fabio");
         assertThat(fabio).isNotNull();
         assertThat(fabio.getNick()).isEqualTo("fax4ever");

         // but indexes have gone!
         verifyMatches(0, "fax4ever");
      });

      execute(IndexStorage.LOCAL_HEAP, true, IndexStartupMode.REINDEX, () -> {
         // data is still present
         Developer fabio = cache.get("fabio");
         assertThat(fabio).isNotNull();
         assertThat(fabio.getNick()).isEqualTo("fax4ever");

         eventually(() -> {
            // now indexes are aligned
            return matches(1, "fax4ever");
         });
      });

      execute(IndexStorage.LOCAL_HEAP, true, IndexStartupMode.NONE, () -> {
         // data is still present
         Developer fabio = cache.get("fabio");
         assertThat(fabio).isNotNull();
         assertThat(fabio.getNick()).isEqualTo("fax4ever");

         // but indexes have gone!
         verifyMatches(0, "fax4ever");
      });

      execute(IndexStorage.LOCAL_HEAP, true, IndexStartupMode.AUTO, () -> {
         // data is still present
         Developer fabio = cache.get("fabio");
         assertThat(fabio).isNotNull();
         assertThat(fabio.getNick()).isEqualTo("fax4ever");

         // auto in this case is equivalent to reindex
         eventually(() -> {
            // now indexes are aligned
            return matches(1, "fax4ever");
         });
      });
   }

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      Util.recursiveFileRemove(fileStoreDataLocation);
      Util.recursiveFileRemove(fileStoreIndexLocation);
      Util.recursiveFileRemove(indexesLocation);
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() {
      Util.recursiveFileRemove(fileStoreDataLocation);
      Util.recursiveFileRemove(fileStoreIndexLocation);
      Util.recursiveFileRemove(indexesLocation);
   }

   private void execute(IndexStorage storage, boolean cacheStorage, IndexStartupMode startupMode, Runnable runnable) {
      try {
         recreateCacheManager(storage, cacheStorage, startupMode);
         runnable.run();
      } finally {
         eventually( () ->
               // Wait for a possible ongoing reindexing
               !Search.getSearchStatistics(cache).getIndexStatistics().reindexing()
         );
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   private void recreateCacheManager(IndexStorage storage, boolean persistentCacheData, IndexStartupMode startupMode) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.indexing()
            .enable()
            .storage(storage)
            .path(indexesLocation)
            .startupMode(startupMode)
            .addIndexedEntity(Developer.class);

      if (persistentCacheData) {
         cfg.persistence()
               .addSoftIndexFileStore()
               .dataLocation(fileStoreDataLocation)
               .indexLocation(fileStoreIndexLocation)
               .preload(true);
      }

      cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cacheManager.getCache();
   }

   private void verifyMatches(int i, String nick) {
      String query = String.format("from %s where nick = '%s'", Developer.class.getName(), nick);
      assertThat(cache.query(query).execute().count().value()).isEqualTo(i);
   }

   private boolean matches(int i, String nick) {
      String query = String.format("from %s where nick = '%s'", Developer.class.getName(), nick);
      HitCount hitCount = cache.query(query).execute().count();
      assertThat(hitCount.exact()).isTrue();
      return hitCount.value() == i;
   }
}
