package org.infinispan.query.config;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "query.config.MultipleIndexingProvidersTest", groups = "functional")
public class MultipleIndexingProvidersTest {

   /**
    * Ensure a single indexing provider per cache is accepted.
    */
   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*It is not allowed to have different indexing configuration.*")
   public void testIndexingProviders() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .addIndexedEntity(TestEntity.class)
            .addProperty("org.infinispan.query.config.MultipleIndexingProvidersTest$TestEntity.directory_provider", "filesystem")
            .addProperty("default.directory_provider", "local-heap");

      builder.build();
   }

   static class TestEntity {
   }
}
