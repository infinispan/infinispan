package org.infinispan.query.dsl.embedded;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Test for non-indexed query on a cache with a single-file store.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.NonIndexedSingleFileStoreQueryDslConditionsTest")
public class NonIndexedSingleFileStoreQueryDslConditionsTest extends NonIndexedQueryDslConditionsTest {

   private final String tmpDirectory = TestingUtil.tmpDirectory(getClass());

   @AfterClass
   @Override
   protected void destroy() {
      try {
         super.destroy();
      } finally {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory);
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.persistence()
            .addStore(SingleFileStoreConfigurationBuilder.class)
            .location(tmpDirectory);

      // ensure the data container contains minimal data so the store will need to be accessed to get the rest
      cfg.locking().concurrencyLevel(1).dataContainer().memory().size(1L);

      createClusteredCaches(1, cfg);
   }
}
