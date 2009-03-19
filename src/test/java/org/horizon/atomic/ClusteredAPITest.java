package org.horizon.atomic;

import org.horizon.Cache;
import static org.horizon.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.horizon.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;
import org.horizon.config.Configuration;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.transaction.DummyTransactionManager;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.List;


@Test(groups = "functional", testName = "atomic.ClusteredAPITest")
public class ClusteredAPITest extends MultipleCacheManagersTest {
   AtomicMapCache cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      c.setInvocationBatchingEnabled(true);

      List<Cache> caches = createClusteredCaches(2, "atomic", c);
      cache1 = (AtomicMapCache) caches.get(0);
      cache2 = (AtomicMapCache) caches.get(1);
   }

   public void testReplicationCommit() throws Exception {
      AtomicMap map = cache1.getAtomicMap("map");

      DummyTransactionManager.getInstance().begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      DummyTransactionManager.getInstance().commit();

      assert map.size() == 2;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");

      assert cache2.getAtomicMap("map").size() == 2;
      assert cache2.getAtomicMap("map").get("blah").equals("blah");
      assert cache2.getAtomicMap("map").containsKey("blah");
   }

   public void testReplicationRollback() throws Exception {
      assertIsEmptyMap(cache2, "map");
      AtomicMap map = cache1.getAtomicMap("map");

      DummyTransactionManager.getInstance().begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      DummyTransactionManager.getInstance().rollback();

      assertIsEmpty(map);
      assertIsEmptyMap(cache1, "map");
      assertIsEmptyMap(cache2, "map");
   }
}
