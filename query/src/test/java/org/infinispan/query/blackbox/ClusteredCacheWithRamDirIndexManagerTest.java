package org.infinispan.query.blackbox;

import org.testng.annotations.Test;

/**
 * Tests the queries on clustered cache which is configured with enabled indexing. The InfinispanIndexManager is enabled,
 * but the directory provider is RAM.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithRamDirIndexManagerTest")
public class ClusteredCacheWithRamDirIndexManagerTest extends ClusteredCacheWithInfinispanDirectoryTest {

   public String getDirectoryProvider() {
      return "ram";
   }
}
