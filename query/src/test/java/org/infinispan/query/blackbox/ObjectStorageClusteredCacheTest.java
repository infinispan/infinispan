package org.infinispan.query.blackbox;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * Verify queries with configured media type as objects.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.blackbox.ObjectStorageClusteredCacheTest")
public class ObjectStorageClusteredCacheTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg
            .encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE)
            .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);
      enhanceConfig(cacheCfg);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
   }

}
