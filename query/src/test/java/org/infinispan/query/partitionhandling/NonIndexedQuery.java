package org.infinispan.query.partitionhandling;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * @since 9.3
 */
@Test(groups = "functional", testName = "query.partitionhandling.NonIndexedQuery")
public class NonIndexedQuery extends SharedIndexTest {

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      return new ConfigurationBuilder();
   }

   @Override
   protected String getQuery() {
      return "from " + Person.class.getName() + " p where p.nonIndexedField = 'Pe'";
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
   }
}
