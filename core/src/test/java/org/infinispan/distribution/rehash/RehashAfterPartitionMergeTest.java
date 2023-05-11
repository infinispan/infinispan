package org.infinispan.distribution.rehash;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName =  "distribution.rehash.RehashAfterPartitionMergeTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class RehashAfterPartitionMergeTest extends MultipleCacheManagersTest {

   Cache<Object, Object> c1, c2;
   List<Cache<Object, Object>> caches;
   DISCARD d1, d2;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, "test",
            getDefaultClusteredCacheConfig(cacheMode),
                  new TransportFlags().withFD(true).withMerge(true));

      caches = caches("test");
      c1 = caches.get(0);
      c2 = caches.get(1);
      d1 = TestingUtil.getDiscardForCache(c1.getCacheManager());
      d2 = TestingUtil.getDiscardForCache(c2.getCacheManager());
   }

   public void testCachePartition() {
      c1.put("1", "value");
      c2.put("2", "value");

      for (Cache<Object, Object> c: caches) {
         assert "value".equals(c.get("1"));
         assert "value".equals(c.get("2"));
         assert manager(c).getMembers().size() == 2;
      }

      d1.discardAll(true);
      d2.discardAll(true);

      // Wait until c1 and c2 have a view of 1 member each
      TestingUtil.blockUntilViewsChanged(60000, 1, c1, c2);

      // we should see a network partition
      for (Cache<Object, Object> c: caches) assert manager(c).getMembers().size() == 1;

      c1.put("3", "value");
      c2.put("4", "value");

      assert "value".equals(c1.get("3"));
      assert null == c2.get("3");

      assert "value".equals(c2.get("4"));
      assert null == c1.get("4");

      // lets "heal" the partition
      d1.discardAll(false);
      d2.discardAll(false);

      // Wait until c1 and c2 have a view of 2 members each
      TestingUtil.blockUntilViewsChanged(45000, 2, c1, c2);

      TestingUtil.waitForNoRebalance(c1, c2);

      c1.put("5", "value");
      c2.put("6", "value");
      for (Cache<Object, Object> c: caches) {
         assert "value".equals(c.get("5"));
         assert "value".equals(c.get("6"));
         assert manager(c).getMembers().size() == 2;
      }
   }

}
