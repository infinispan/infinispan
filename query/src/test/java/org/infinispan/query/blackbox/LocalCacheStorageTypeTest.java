package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
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

   @Override
   protected String parameters() {
      return "[" + storageType + "]";
   }

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
            .enable()
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      cfg.memory()
            .storageType(storageType);
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }
}
