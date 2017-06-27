package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "api.ScatteredConcurrentOperationsTest")
public class ScatteredConcurrentOperationsTest extends ConcurrentOperationsTest {
   public ScatteredConcurrentOperationsTest() {
      super(CacheMode.SCATTERED_SYNC, 2, 2, 4);
   }

   @Override
   protected boolean checkOwners(List<Address> owners) {
      assert owners.size() == 1;

      InternalCacheEntry entry0 = null;
      EntryVersion version = null;
      InternalCacheEntry entry1 = null;
      Address backupOwner = null;
      for (Cache c : caches()) {
         InternalCacheEntry entry = c.getAdvancedCache().getDataContainer().peek("k");
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(owners.get(0))) {
            entry0 = entry;
         } else if (entry != null && entry.getMetadata() != null && entry.getMetadata().version() != null) {
            if (version == null || version.compareTo(entry.getMetadata().version()) == InequalVersionComparisonResult.BEFORE) {
               version = entry.getMetadata().version();
               entry1 = entry;
               backupOwner = c.getAdvancedCache().getRpcManager().getAddress();
            }
         }
      }

      return checkOwnerEntries(entry0, entry1, owners.get(0), backupOwner);
   }
}
