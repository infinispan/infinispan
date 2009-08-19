package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = "functional", testName = "replication.BaseReplicatedAPITest")
public abstract class BaseReplicatedAPITest extends MultipleCacheManagersTest {

   AdvancedCache cache1, cache2;
   protected boolean isSync;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(isSync ? Configuration.CacheMode.REPL_SYNC : Configuration.CacheMode.REPL_ASYNC);
      c.setStateRetrievalTimeout(1000);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      List<Cache<Object, Object>> caches = createClusteredCaches(2, "replication", c);
      cache1 = caches.get(0).getAdvancedCache();
      cache2 = caches.get(1).getAdvancedCache();
   }

   public void put() {
      // test a simple put!
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.put("key", "value");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      Map map = new HashMap();
      map.put("key2", "value2");
      map.put("key3", "value3");

      replListener(cache2).expect(PutMapCommand.class);
      cache1.putAll(map);
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key2").equals("value2");
      assert cache2.get("key2").equals("value2");
      assert cache1.get("key3").equals("value3");
      assert cache2.get("key3").equals("value3");
   }

   public void remove() {
      cache2.put("key", "value", Flag.CACHE_MODE_LOCAL);
      assert cache2.get("key").equals("value");
      assert cache1.get("key") == null;

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      cache1.put("key", "value", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testPutIfAbsent() {
      cache2.put("key", "valueOld", Flag.CACHE_MODE_LOCAL);
      assert cache2.get("key").equals("valueOld");
      assert cache1.get("key") == null;

      replListener(cache2).expect(PutKeyValueCommand.class);
      cache1.putIfAbsent("key", "value");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2");

      cache1.putIfAbsent("key", "value3");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value2"); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      cache1.put("key", "value1", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.remove("key", "value");

      assert cache1.get("key").equals("value1") : "Should not remove";
      assert cache2.get("key").equals("value2") : "Should not remove";

      replListener(cache2).expect(RemoveCommand.class);
      cache1.remove("key", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testClear() {
      cache1.put("key", "value1", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(ClearCommand.class);
      cache1.clear();
      replListener(cache2).waitForRpc();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testReplace() {
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.put("key", "valueN", Flag.CACHE_MODE_LOCAL);

      replListener(cache2).expect(ReplaceCommand.class);
      cache1.replace("key", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }

   public void testReplaceWithOldVal() {
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.put("key", "valueN", Flag.CACHE_MODE_LOCAL);

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key").equals("valueN");
      assert cache2.get("key").equals("value2");

      replListener(cache2).expect(ReplaceCommand.class);
      cache1.replace("key", "valueN", "value1");
      replListener(cache2).waitForRpc();

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }
   
   public void testLocalOnlyClear() {
      cache1.put("key", "value1", Flag.CACHE_MODE_LOCAL);
      cache2.put("key", "value2", Flag.CACHE_MODE_LOCAL);
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.clear(Flag.CACHE_MODE_LOCAL);

      assert cache1.get("key") == null;
      assert cache2.get("key") != null;
      assert cache2.get("key").equals("value2");
   }
}
