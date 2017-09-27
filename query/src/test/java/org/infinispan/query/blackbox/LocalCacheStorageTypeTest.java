package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author vjuranek
 * @since 9.2
 */
@Test(groups = {"functional", "smoke"}, testName = "query.blackbox.LocalCacheStorageTypeTest")
public class LocalCacheStorageTypeTest extends LocalCacheTest {

   protected StorageType storageType;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new LocalCacheStorageTypeTest().withStorageType(StorageType.OFF_HEAP),
            new LocalCacheStorageTypeTest().withStorageType(StorageType.BINARY),
            new LocalCacheStorageTypeTest().withStorageType(StorageType.OBJECT),
      };
   }

   LocalCacheStorageTypeTest withStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cfg.memory()
            .storageType(storageType);
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

}
