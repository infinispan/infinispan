package org.infinispan.tx;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "tx.NoAutoCommitAndPferTest")
public class NoAutoCommitAndPferTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dsc = getDefaultStandaloneCacheConfig(true);
      dsc.transaction().autoCommit(false);
      return TestCacheManagerFactory.createCacheManager(dsc);
   }

   public void testPferNoAutoCommitExplicitTransaction() throws Exception {
      tm().begin();
      cache.putForExternalRead("k1","v");
      tm().commit();
      assertEquals("v", cache.get("k1")); //here is the failure!
   }

   public void testPferNoAutoCommit() throws Exception {
      cache.putForExternalRead("k2","v");
      assertEquals("v", cache.get("k2")); //here is the failure!
   }

}
