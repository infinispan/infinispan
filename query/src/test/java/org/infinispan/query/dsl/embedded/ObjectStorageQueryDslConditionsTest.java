package org.infinispan.query.dsl.embedded;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify query DSL when configuring object storage. Just a smoke test that executes in embedded mode and no interaction is done
 * via a remote client. This just ensures nothing gets broken on the embedded side if encoding configuration is active.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.ObjectStorageQueryDslConditionsTest")
public class ObjectStorageQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfg.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass())
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }
}
