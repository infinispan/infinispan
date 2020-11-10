package org.infinispan.query.partitionhandling;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.NonSharedIndexWithLocalCachesTest")
public class NonSharedIndexWithLocalCachesTest extends NonSharedIndexTest {

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      return configurationBuilder;
   }

}
