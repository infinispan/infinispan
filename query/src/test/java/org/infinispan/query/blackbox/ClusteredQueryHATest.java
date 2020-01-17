package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Index;
import org.testng.annotations.Test;

/**
 * Tests for query broadcasting when storing indexes redundantly: DIST caches with Index.ALL
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryHATest")
public class ClusteredQueryHATest extends ClusteredQueryTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }

   @Override
   protected Index getIndexMode() {
      return Index.ALL;
   }
}
