package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
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
import org.infinispan.Version;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.commons.util.Features;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.DataContainerFactory;
import org.infinispan.jboss.marshalling.core.JBossUserMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(testName = "org.infinispan.tools.store.migrator.AbstractReaderTest", groups = "functional")
public abstract class AbstractReaderTest extends AbstractInfinispanTest {

   private static final String TEST_CACHE_NAME = "reader-test";

   protected int majorVersion = 8;
   protected int segmentCount;

   public <T extends AbstractReaderTest> T majorVersion(int majorVersion) {
      this.majorVersion = majorVersion;
      return (T) this;
   }

   public <T extends AbstractReaderTest> T segmented(int segmentCount) {
      this.segmentCount = segmentCount;
      return (T) this;
   }

   @Override
   protected String parameters() {
      return String.format("[version=%d,segments=%d]", majorVersion, segmentCount);
   }

   /**
    * Method meant to be overriden to add store configuration. The default implementation already handles configuring
    * the segments for the hash and thus the store only needs to enable segmenting on the store if applicable.
    * @return builder that should make the target config
    */
   protected ConfigurationBuilder getTargetCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (segmentCount > 0) {
         builder.clustering().hash().numSegments(segmentCount);
      }
      return builder;
   }

   protected void configureStoreProperties(Properties properties, Element type) {
      // User externalizers must start at JBossUserMarshaller.USER_EXT_ID_MIN in Infinispan 10.x
      int externalizerId = type == TARGET ? JBossUserMarshaller.USER_EXT_ID_MIN : 256;
      properties.put(propKey(type, CACHE_NAME), TEST_CACHE_NAME);
      properties.put(propKey(type, MARSHALLER, EXTERNALIZERS), externalizerId + ":" + TestUtil.TestObjectExternalizer.class.getName());
      properties.put(propKey(type, VERSION), type == SOURCE ? String.valueOf(majorVersion): Version.getMajor());

      if (type == TARGET && segmentCount > 0) {
         properties.put(propKey(type, SEGMENT_COUNT), String.valueOf(segmentCount));
      }
   }

   @Test
   public void readerCompatibilityTest() throws Exception {
      boolean segmentationAvailable = new Features().isAvailable(DataContainerFactory.SEGMENTATION_FEATURE);
      SkipTestNG.skipIf(segmentCount > 0 && !segmentationAvailable, "Segmentation is disabled");

      Properties properties = new Properties();
      configureStoreProperties(properties, SOURCE);
      configureStoreProperties(properties, TARGET);
      new StoreMigrator(properties).run();

      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      globalConfig.serialization().addAdvancedExternalizer(JBossUserMarshaller.USER_EXT_ID_MIN, new TestUtil.TestObjectExternalizer());

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
