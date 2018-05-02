package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tools.store.migrator.marshaller.MarshallerType;
import org.testng.annotations.Test;

@Test(testName = "org.infinispan.tools.store.migrator.AbstractReaderTest", groups = "functional")
public abstract class AbstractReaderTest {

   private static final String TEST_CACHE_NAME = "reader-test";

   abstract public Configuration getTargetCacheConfig();

   protected void configureStoreProperties(Properties properties, Element type) {
      MarshallerType marshallerType = type == SOURCE ? MarshallerType.LEGACY : MarshallerType.CURRENT;
      properties.put(propKey(type, CACHE_NAME), TEST_CACHE_NAME);
      properties.put(propKey(type, MARSHALLER, TYPE), marshallerType.toString());
      properties.put(propKey(type, MARSHALLER, EXTERNALIZERS), "256:" + TestUtil.TestObjectExternalizer.class.getName());
   }

   @Test
   public void readerCompatibilityTest() throws Exception {
      Properties properties = new Properties();
      configureStoreProperties(properties, SOURCE);
      configureStoreProperties(properties, TARGET);
      // Read from the legacy LevelDB store and populate the new RocksDBStore using latest marshaller
      new StoreMigrator(properties).run();

      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .serialization().addAdvancedExternalizer(256, new TestUtil.TestObjectExternalizer())
            .build();

      Configuration config = getTargetCacheConfig();

      // Create a new cache instance, with the required externalizers, to ensure that the new RocksDbStore can be
      // loaded and contains all of the expected values.
      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig, config);
      Cache cache = manager.getCache(TEST_CACHE_NAME);
      for (String key : TestUtil.TEST_MAP.keySet()) {
         Object stored = cache.get(key);
         assertNotNull(String.format("Key=%s", key), stored);
         Object expected = TestUtil.TEST_MAP.get(key);
         assertNotNull(String.format("Key=%s", key), stored);
         assertEquals(expected, stored);
      }
   }
}
