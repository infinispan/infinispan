package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "atomic.ClusteredAPITest")
public class ClusteredAPITest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      c.setInvocationBatchingEnabled(true);

      createClusteredCaches(2, "atomic", c);

   }

   public void testReplicationCommit() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

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
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");
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
