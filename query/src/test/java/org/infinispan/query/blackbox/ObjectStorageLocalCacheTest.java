package org.infinispan.query.blackbox;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
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
            .enable()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
