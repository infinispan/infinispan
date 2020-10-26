package org.infinispan.test.integration.store;

import static java.io.File.separator;
import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the Infinispan RocksDB CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public abstract class AbstractInfinispanStoreRocksDBIT {
   private static String baseDir = tmpDirectory(AbstractInfinispanStoreRocksDBIT.class);
   private static String dataDir = baseDir + separator + "data";
   private static String expiredDir = baseDir + separator + "expired";

   private EmbeddedCacheManager cm;

   @Before
   @After
   public void removeDataFilesIfExists() {
      Util.recursiveFileRemove(baseDir);
      if (cm != null)
         cm.stop();
   }

   /**
    * To avoid pulling in TestingUtil and its plethora of dependencies
    */
   private static String tmpDirectory(Class<?> test) {
      String prefix = System.getProperty("infinispan.test.tmpdir", System.getProperty("java.io.tmpdir"));
      return prefix + separator + "infinispanTempFiles" + separator + test.getSimpleName();
   }

   @Test
   public void testCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalState().persistentLocation(baseDir).defaultCacheName("default");
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class)
            .location(dataDir)
            .expiredLocation(expiredDir);

      cm = new DefaultCacheManager(gcb.build(), builder.build());
      Cache<String, String> cache = cm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }
}
