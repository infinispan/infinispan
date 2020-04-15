package org.infinispan.query.config;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "query.config.MultipleIndexingProvidersTest", groups = "functional")
public class MultipleIndexingProvidersTest {

   /**
    * Ensure a single indexing provider per cache is accepted.
    */
   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*ISPN000582: A single indexing directory provider is allowed.*")
   public void testIndexingProviders() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
             .addIndexedEntity(TestEntity.class)
             .addProperty("org.infinispan.query.config.MultipleIndexingProvidersTest$TestEntity.directory.type", "filesystem")
             .addProperty("directory.type", "local-heap");

      builder.build();
   }

   static class TestEntity {
   }
}
