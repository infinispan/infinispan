package org.infinispan.query.dsl.embedded;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify non-indexed query when configuring application/x-java-object storage.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ObjectStorageNonIndexedQueryDslConditionsTest")
public class ObjectStorageNonIndexedQueryDslConditionsTest extends NonIndexedQueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      createClusteredCaches(1, cfg);
   }
}
