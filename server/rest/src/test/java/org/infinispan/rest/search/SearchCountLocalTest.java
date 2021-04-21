package org.infinispan.rest.search;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @since 12.1
 */
@Test(groups = "functional", testName = "rest.search.SearchCountLocalTest")
public class SearchCountLocalTest extends SearchCountClusteredTest {

   @Override
   int getMembers() {
      return 1;
   }

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.LOCAL;
   }
}
