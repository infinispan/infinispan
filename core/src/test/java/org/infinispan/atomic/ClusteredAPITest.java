package org.infinispan.atomic;

import org.infinispan.Cache;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.DummyTransactionManager;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.List;


@Test(groups = "functional", testName = "atomic.ClusteredAPITest")
public class ClusteredAPITest extends MultipleCacheManagersTest {
   AtomicMapCache cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      c.setInvocationBatchingEnabled(true);

      List<Cache<Object, Object>> caches = createClusteredCaches(2, "atomic", c);
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
