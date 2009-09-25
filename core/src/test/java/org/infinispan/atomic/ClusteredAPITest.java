package org.infinispan.atomic;

import org.infinispan.Cache;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.List;


@Test(groups = "functional", testName = "atomic.ClusteredAPITest")
public class ClusteredAPITest extends MultipleCacheManagersTest {
   Cache<String, Object> cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      c.setInvocationBatchingEnabled(true);

      List<Cache<String, Object>> caches = createClusteredCaches(2, "atomic", c);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   public void testReplicationCommit() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache1, "map");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 2;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");

      assert AtomicMapLookup.getAtomicMap(cache2, "map").size() == 2;
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get("blah").equals("blah");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey("blah");
   }

   public void testReplicationRollback() throws Exception {
      assertIsEmptyMap(cache2, "map");
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache1, "map");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      TestingUtil.getTransactionManager(cache1).rollback();

      assertIsEmpty(map);
      assertIsEmptyMap(cache1, "map");
      assertIsEmptyMap(cache2, "map");
   }
}
