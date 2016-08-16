package org.infinispan.atomic;

import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmpty;
import static org.infinispan.atomic.AtomicHashMapTestAssertions.assertIsEmptyMap;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "atomic.AtomicMapReplTest")
public class AtomicMapReplTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.invocationBatching().enable();
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

   public void testMultipleReplicationCommit(Method m) throws Exception {
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
      assert map.get("existing").equals("existing");
      assert map.containsKey("existing");

      assert AtomicMapLookup.getAtomicMap(cache2, "map").size() == 2;
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get("blah").equals("blah");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey("blah");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get("existing").equals("existing");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey("existing");

      map = AtomicMapLookup.getAtomicMap(cache1, "map");
      TestingUtil.getTransactionManager(cache1).begin();
      String newKey = "k-" + m.getName();
      String newValue = "v-" + m.getName();
      map.put(newKey, newValue);
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 3;
      assert map.get("blah").equals("blah");
      assert map.containsKey("blah");
      assert map.get("existing").equals("existing");
      assert map.containsKey("existing");
      assert map.get(newKey).equals(newValue);
      assert map.containsKey(newKey);

      assert AtomicMapLookup.getAtomicMap(cache2, "map").size() == 3;
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get("blah").equals("blah");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey("blah");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get("existing").equals("existing");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey("existing");
      assert AtomicMapLookup.getAtomicMap(cache2, "map").get(newKey).equals(newValue);
      assert AtomicMapLookup.getAtomicMap(cache2, "map").containsKey(newKey);
   }

   public void testReplicationCommitCreateMapInTransaction(Method m) throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      TestingUtil.getTransactionManager(cache1).begin();
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache1, m.getName());
      map.put("a", "b");
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 1;
      assert map.get("a").equals("b");
      assert map.containsKey("a");

      assert AtomicMapLookup.getAtomicMap(cache2, m.getName()).size() == 1;
      assert AtomicMapLookup.getAtomicMap(cache2, m.getName()).get("a").equals("b");
      assert AtomicMapLookup.getAtomicMap(cache2, m.getName()).containsKey("a");
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
