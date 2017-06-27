package org.infinispan.scattered.store;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistSyncStoreNotSharedTest;
import org.infinispan.scattered.Utils;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class ScatteredSyncStoreNotSharedTest extends DistSyncStoreNotSharedTest {
   public ScatteredSyncStoreNotSharedTest() {
      numOwners = 1;
      l1CacheEnabled = false;
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder cb = super.buildConfiguration();
      cb.clustering().cacheMode(CacheMode.SCATTERED_SYNC);
      return cb;
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      Utils.assertOwnershipAndNonOwnership(caches, key);
   }

   @Override
   protected void assertIsNotInL1(Cache<?, ?> cache, Object key) {
   }

   @Override
   protected void assertInStores(String key, String value, boolean allowL1) {
      Utils.assertInStores(caches, key, value);
   }

   @Test(enabled = false, description = "get() does not update in-memory value from cache-store on non-owner")
   @Override
   public void testGetFromNonOwnerWithFlags(Method m) throws Exception {
   }
}
