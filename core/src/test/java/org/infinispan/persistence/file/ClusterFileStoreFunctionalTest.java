package org.infinispan.persistence.file;

import java.io.File;
import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "unit", testName = "persistence.file.ClusterFileStoreFunctionalTest")
public class ClusterFileStoreFunctionalTest extends MultipleCacheManagersTest {

   // createCacheManager executes before any @BeforeClass defined in the class, so simply use standard tmp folder.
   private final String tmpDirectory = TestingUtil.tmpDirectory(this);
   private static final String CACHE_NAME = "clusteredFileCacheStore";

   private Cache<Object, ?> cache1, cache2;

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                                                       new ConfigurationBuilder());
      EmbeddedCacheManager cacheManager2 = createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(),
                                                                       new ConfigurationBuilder());
      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder builder1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      addCacheStoreConfig(builder1, 1);
      ConfigurationBuilder builder2 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      addCacheStoreConfig(builder2, 2);

      cacheManager1.defineConfiguration(CACHE_NAME, builder1.build());
      cacheManager2.defineConfiguration(CACHE_NAME, builder2.build());
      cache1 = cache(0, CACHE_NAME);
      cache2 = cache(1, CACHE_NAME);
   }

   public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
      final Object mapKey = m.getName();
      TransactionManager tm = cache1.getAdvancedCache().getTransactionManager();
      tm.begin();
      final AtomicMap<String, String> map = getAtomicMap(cache1, mapKey);
      map.put("a", "b");
      tm.commit();

      //ISPN-3161 => the eviction tries to acquire the lock, however the TxCompletionNotificationCommand is sent async
      //             and the deliver can be delayed resulting in a delay releasing the lock and a TimeoutException
      //             when the evict tries to acquire the lock.
      assertEventuallyNoLocksAcquired(mapKey);

      //evict from memory
      cache1.evict(mapKey);

      // now re-retrieve the map and make sure we see the diffs
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache1, mapKey).get("a"));
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache2, mapKey).get("a"));

      cache2.evict(mapKey);
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache1, mapKey).get("a"));
      assertEquals("Wrong value for key [a] in atomic map.", "b", getAtomicMap(cache2, mapKey).get("a"));
   }

   protected void addCacheStoreConfig(ConfigurationBuilder builder, int index) {
      builder.persistence().addSingleFileStore().location(tmpDirectory + "/" + index);
   }

   protected void assertEventuallyNoLocksAcquired(final Object key) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache1).isLocked(key);
         }
      });
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache2).isLocked(key);
         }
      });
   }

}
