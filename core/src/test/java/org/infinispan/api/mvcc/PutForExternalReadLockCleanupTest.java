package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional")
@CleanupAfterMethod
public abstract class PutForExternalReadLockCleanupTest extends MultipleCacheManagersTest {

   private static final String VALUE = "v";

   public void testLockCleanupOnBackup() {
      doTest(false);
   }

   public void testLockCleanuponOwner() {
      doTest(true);
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional());
      c.clustering().hash().numSegments(10).numOwners(1);
      c.clustering().l1().disable();
      amendConfiguration(c);
      createClusteredCaches(2, c);
   }

   protected abstract boolean transactional();

   protected abstract void amendConfiguration(ConfigurationBuilder builder);

   private void doTest(boolean owner) {
      final Cache<MagicKey, String> cache1 = cache(0);
      final Cache<MagicKey, String> cache2 = cache(1);
      final MagicKey magicKey = new MagicKey(cache1);

      if (owner) {
         cache1.putForExternalRead(magicKey, VALUE);
      } else {
         cache2.putForExternalRead(magicKey, VALUE);
      }

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.containsKey(magicKey) && cache2.containsKey(magicKey);
         }
      });
      assertEquals(VALUE, cache1.get(magicKey));
      assertEquals(VALUE, cache2.get(magicKey));
      assertNotLocked(magicKey);
   }
}
