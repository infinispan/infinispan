package org.infinispan.tools.store.migrator.rocksdb;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.LOCATION;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;

import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.tools.store.migrator.Element;
import org.infinispan.tools.store.migrator.StoreMigrator;
import org.infinispan.tools.store.migrator.StoreType;
import org.infinispan.tools.store.migrator.TestUtil;
import org.infinispan.tools.store.migrator.marshaller.MarshallerType;
import org.testng.annotations.Test;

@Test(testName = "tools.store.migrator.RocksDBReaderTest", groups = "functional")
public class RocksDBReaderTest {

   private static final String TEST_CACHE_NAME = "leveldbstore";
   private static final String SOURCE_DIR = "target/test-classes/";
   private static final String TARGET_DIR = SOURCE_DIR + "/rocksdbstore/";

   public void testLevelDbCompatibility() throws Exception {
      Properties properties = new Properties();
      configureStoreProperties(properties, SOURCE);
      configureStoreProperties(properties, TARGET);
      // Read from the legacy LevelDB store and populate the new RocksDBStore using latest marshaller
      new StoreMigrator(properties).run();

      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .serialization().addAdvancedExternalizer(256, new TestUtil.TestObjectExternalizer())
            .build();

      Configuration config = new ConfigurationBuilder().persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class).location(TARGET_DIR).expiredLocation(TARGET_DIR + "-expired-")
            .preload(true).ignoreModifications(true)
            .build();

      // Create a new cache instance, with the required externalizers, to ensure that the new RocksDbStore can be
      // loaded and contains all of the expected values.
      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig, config);
      Cache cache = manager.getCache(TEST_CACHE_NAME);
      for (String key : TestUtil.TEST_MAP.keySet()) {
         Object stored = cache.get(key);
         assert stored != null;
         Object expected = TestUtil.TEST_MAP.get(key);
         assert expected != null;
         assert expected.equals(stored);
      }
   }

   private void configureStoreProperties(Properties properties, Element type) {
      MarshallerType marshallerType = type == SOURCE ? MarshallerType.LEGACY : MarshallerType.CURRENT;
      properties.put(propKey(type, TYPE), StoreType.ROCKSDB.toString());
      properties.put(propKey(type, CACHE_NAME), TEST_CACHE_NAME);
      properties.put(propKey(type, LOCATION), type == SOURCE ? SOURCE_DIR : TARGET_DIR);
      properties.put(propKey(type, MARSHALLER, TYPE), marshallerType.toString());
      properties.put(propKey(type, MARSHALLER, EXTERNALIZERS), "256:" + TestUtil.TestObjectExternalizer.class.getName());
   }
}
