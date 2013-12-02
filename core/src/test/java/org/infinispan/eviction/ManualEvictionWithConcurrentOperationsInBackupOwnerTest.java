package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Tests manual eviction with concurrent read and/or write operation. This test has passivation disabled and the
 * eviction happens in the backup owner
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithConcurrentOperationsInBackupOwnerTest", singleThreaded = true)
public class ManualEvictionWithConcurrentOperationsInBackupOwnerTest
      extends ManualEvictionWithConcurrentOperationsInPrimaryOwnerTest {

   @Override
   protected Object createSameHashCodeKey(String name) {
      final Cache otherCache = otherCacheManager.getCache();
      final Address address = otherCache.getAdvancedCache().getRpcManager().getAddress();
      DistributionManager distributionManager = otherCache.getAdvancedCache().getDistributionManager();
      int hashCode = 0;
      SameHashCodeKey key = new SameHashCodeKey(name, hashCode);
      while (!distributionManager.getPrimaryLocation(key).equals(address)) {
         hashCode++;
         key = new SameHashCodeKey(name, hashCode);
      }
      return key;
   }
}
