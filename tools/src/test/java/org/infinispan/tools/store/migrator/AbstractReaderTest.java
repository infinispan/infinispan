package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.CONTEXT_INITIALIZERS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SEGMENT_COUNT;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.VERSION;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(testName = "org.infinispan.tools.store.migrator.AbstractReaderTest", groups = "functional")
public abstract class AbstractReaderTest extends AbstractInfinispanTest {

   private static final String TEST_CACHE_NAME = "reader-test";

   protected int majorVersion = 8;
   protected int sourceSegments;
   protected int targetSegments;

   public <T extends AbstractReaderTest> T majorVersion(int majorVersion) {
      this.majorVersion = majorVersion;
      return (T) this;
   }

   public <T extends AbstractReaderTest> T sourceSegments(int segmentCount) {
      this.sourceSegments = segmentCount;
      return (T) this;
   }

   public <T extends AbstractReaderTest> T targetSegments(int segmentCount) {
      this.targetSegments = segmentCount;
      return (T) this;
   }

   @Override
   protected String parameters() {
      return String.format("[version=%d,sourceSegments%d,targetSegments=%d]", majorVersion, sourceSegments, targetSegments);
   }

   /**
    * Method meant to be overriden to add store configuration. The default implementation already handles configuring
    * the segments for the hash and thus the store only needs to enable segmenting on the store if applicable.
    * @return builder that should make the target config
    */
   protected ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (targetSegments > 0) {
         builder.clustering().hash().numSegments(targetSegments);
      }
      return builder;
   }

   protected void configureStoreProperties(Properties properties, Element type) {
      properties.put(propKey(type, CACHE_NAME), TEST_CACHE_NAME);
      if (type == SOURCE) {
         if (majorVersion < 10) {
            properties.put(propKey(type, MARSHALLER, EXTERNALIZERS), 256 + ":" + TestUtil.TestObjectExternalizer.class.getName());
         } else {
            properties.put(propKey(type, MARSHALLER, CONTEXT_INITIALIZERS), TestUtil.SCI.INSTANCE.getClass().getName());
         }

         if (sourceSegments > 0)
            properties.put(propKey(type, SEGMENT_COUNT), Integer.toString(sourceSegments));
      } else {
         properties.put(propKey(type, MARSHALLER, CONTEXT_INITIALIZERS), TestUtil.SCI.INSTANCE.getClass().getName());
      }
      properties.put(propKey(type, VERSION), type == SOURCE ? String.valueOf(majorVersion): Version.getMajor());

      if (type == TARGET && targetSegments > 0) {
         properties.put(propKey(type, SEGMENT_COUNT), String.valueOf(targetSegments));
      }
   }

   @Test
   public void readerCompatibilityTest() throws Exception {
      Properties properties = new Properties();
      configureStoreProperties(properties, SOURCE);
      configureStoreProperties(properties, TARGET);
      new StoreMigrator(properties).run();

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      globalConfig.serialization().addContextInitializer(TestUtil.SCI.INSTANCE);
      globalConfig.defaultCacheName(TEST_CACHE_NAME);

      // Create a new cache instance, with the required externalizers, to ensure that the new RocksDbStore can be
      // loaded and contains all of the expected values.
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(globalConfig, getTargetCacheConfig());
      try {
         Cache<String, Object> cache = manager.getCache(TEST_CACHE_NAME);
         for (String key : TestUtil.TEST_MAP.keySet()) {
            Object stored = cache.get(key);
            assertNotNull(String.format("Key=%s", key), stored);
            Object expected = TestUtil.TEST_MAP.get(key);
            assertNotNull(String.format("Key=%s", key), stored);
            assertEquals(expected, stored);
         }

         // Ensure that all of the unsupported classes are not written to the target store
         TestUtil.TEST_MAP_UNSUPPORTED.keySet().stream()
               .map(cache::get)
               .forEach(AssertJUnit::assertNull);
      } finally {
         manager.stop();
      }
   }
}
