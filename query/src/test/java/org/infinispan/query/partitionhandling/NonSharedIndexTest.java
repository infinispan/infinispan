package org.infinispan.query.partitionhandling;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.NonSharedIndexTest")
public class NonSharedIndexTest extends SharedIndexTest {

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return configurationBuilder;
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
   }
}
