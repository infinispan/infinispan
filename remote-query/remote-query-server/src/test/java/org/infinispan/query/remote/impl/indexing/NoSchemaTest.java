package org.infinispan.query.remote.impl.indexing;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@SuppressWarnings("resource")
@Test(groups = "functional", testName = "query.remote.impl.indexing.NoSchemaTest")
public class NoSchemaTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(configurationBuilder());
      waitForClusterToForm();
   }

   private static ConfigurationBuilder configurationBuilder() {
      var builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash().numSegments(20).numOwners(1);
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User");
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM);
      return builder;
   }

   public void testNodeJoiningAndLeaving() {
      assertEquals(0, cache(0).size());
      assertTrue(cache(0).getCacheConfiguration().indexing().enabled());
      addClusterEnabledCacheManager(configurationBuilder());
      waitForClusterToForm();
      // gets should not throw exception since they don't use indexes.
      assertNull(cache(0).get("not_found"));
      assertNull(cache(1).get("not_found"));

      // kind useless test since the exception during stop is only logged and never thrown.
      killMember(1);
      waitForClusterToForm();
   }

   public void testClear() {
      // clear should not throw any exception
      cache(0).clear();
      assertEquals(0, cache(0).size());
      assertTrue(cache(0).getCacheConfiguration().indexing().enabled());
   }
}
