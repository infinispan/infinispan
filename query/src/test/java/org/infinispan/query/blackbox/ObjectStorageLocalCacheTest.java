package org.infinispan.query.blackbox;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify queries when configuring storage as application/x-java-objects.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.blackbox.ObjectStorageLocalCacheTest")
public class ObjectStorageLocalCacheTest extends LocalCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE)
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
