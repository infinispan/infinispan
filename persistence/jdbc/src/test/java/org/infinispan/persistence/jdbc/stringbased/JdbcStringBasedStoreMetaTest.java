package org.infinispan.persistence.jdbc.stringbased;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test to ensure that JDBC meta table validation when loading existing stores.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreMetaTest")
public class JdbcStringBasedStoreMetaTest extends AbstractInfinispanTest {

   public void testSegmentCompatibility() {
      // Create the original store
      createCacheManager(true, 10);
      try {
         // Attempt to use the original store with a different number of segments
         createCacheManager(false, 15);
      } catch (EmbeddedCacheManagerStartupException e) {
         assertNotNull(e);
         Throwable cause = e.getCause();
         assertNotNull(cause);
         assertTrue(cause instanceof CacheConfigurationException);
      }
   }

   private void createCacheManager(boolean createOnStart, int segments) {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cfg.clustering().hash().numSegments(segments);

      JdbcStringBasedStoreConfigurationBuilder store = cfg
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .fetchPersistentState(true);
      store.table().createOnStart(createOnStart);
      UnitTestDatabaseManager.buildTableManipulation(store.table());

      store.simpleConnection()
            .driverClass(org.h2.Driver.class)
            .connectionUrl(String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", JdbcStringBasedStoreMetaTest.class))
            .username("sa");
      withCacheManager(() -> TestCacheManagerFactory.createCacheManager(cfg), Void -> {});
   }
}
