package org.infinispan.query.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.persistence.SharedCacheLoaderQueryDistributedIndexTest", enabled = false,
      description = "Temporary disabled: https://issues.jboss.org/browse/ISPN-2249 , https://issues.jboss.org/browse/ISPN-1586")
public class SharedCacheLoaderQueryDistributedIndexTest extends SharedCacheLoaderQueryIndexTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);

      builder.indexing().enable().indexLocalOnly(true)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_36")
            .addProperty("default.exclusive_index_use", "false");
   }
}
