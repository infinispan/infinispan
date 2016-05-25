package org.infinispan.scattered;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;

import java.util.List;

import static org.infinispan.distribution.DistributionTestHelper.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Utils {
   public static void assertOwnershipAndNonOwnership(List<? extends Cache> caches, Object key) {
      EntryVersion ownerVersion = null;
      for (Cache c: caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.peek(key);
         if (isOwner(c, key)) {
            assert ice != null : "Fail on owner cache " + addressOf(c) + ": dc.get(" + key + ") returned null!";
            assert ice instanceof ImmortalCacheEntry : "Fail on owner cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(ice);
            ownerVersion = ice.getMetadata().version();
         }
      }
      assertNotNull(ownerVersion);
      if (caches.size() == 1) {
         return;
      }
      int equalVersions = 0;
      for (Cache c: caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.peek(key);
         if (!isOwner(c, key)) {
            if (ice == null) continue;
            if (ice != null && ice.getMetadata() != null && ownerVersion.equals(ice.getMetadata().version())) ++equalVersions;
         }
      }
      assertEquals(equalVersions, 1);
   }

   public static void assertInStores(List<? extends Cache> caches, String key, String value) {
      EntryVersion ownerVersion = null;
      for (Cache c: caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (isOwner(c, key)) {
            assertIsInContainerImmortal(c, key);
            MarshalledEntry me = store.load(key);
            assertEquals(me.getValue(), value);
            ownerVersion = me.getMetadata().version();
         }
      }
      assertNotNull(ownerVersion);
      if (caches.size() == 1) {
         return;
      }
      int equalVersions = 0;
      for (Cache c: caches) {
         CacheLoader store = TestingUtil.getFirstLoader(c);
         if (!isOwner(c, key)) {
            MarshalledEntry me = store.load(key);
            if (me != null && me.getMetadata() != null && ownerVersion.equals(me.getMetadata().version())) {
               assertEquals(me.getValue(), value);
               ++equalVersions;
            }
         }
      }
      assertEquals(equalVersions, 1);
   }
}
