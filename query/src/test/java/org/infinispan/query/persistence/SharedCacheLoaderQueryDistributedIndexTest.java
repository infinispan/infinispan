package org.infinispan.query.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.persistence.SharedCacheLoaderQueryDistributedIndexTest")
public class SharedCacheLoaderQueryDistributedIndexTest extends SharedCacheLoaderQueryIndexTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);

      builder.indexing().enable()
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

   }
}
