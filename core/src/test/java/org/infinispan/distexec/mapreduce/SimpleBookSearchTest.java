package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests the mapreduce with simple configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.SimpleBookSearchTest")
public class SimpleBookSearchTest extends BookSearchTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createClusteredCaches(4, "bookSearch" ,builder);
   }
}
