package org.infinispan.replication;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class ReplicatedAPITest extends MultipleCacheManagersTest {

   protected boolean isSync;

   public ReplicatedAPITest sync(boolean sync) {
      this.isSync = sync;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] { new ReplicatedAPITest().sync(true), new ReplicatedAPITest().sync(false) };
   }

   @Override
   protected String parameters() {
      return "{sync=" + isSync + "}";
   }

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder build = getDefaultClusteredCacheConfig(isSync ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC, true);
      build.clustering().stateTransfer().timeout(10000);
      createClusteredCaches(2, "replication", build);
   }

   public void put() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      // test a simple put!
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      expectRpc(cache2, PutKeyValueCommand.class);
      cache1.put("key", "value");
      waitForRpc(cache2);

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      Map map = new HashMap();
      map.put("key2", "value2");
      map.put("key3", "value3");

      expectRpc(cache2, PutMapCommand.class);
      cache1.putAll(map);
      waitForRpc(cache2);

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key2").equals("value2");
      assert cache2.get("key2").equals("value2");
      assert cache1.get("key3").equals("value3");
      assert cache2.get("key3").equals("value3");
   }

   public void remove() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key") == null;

      expectRpc(cache2, RemoveCommand.class);
      cache1.remove("key");
      waitForRpc(cache2);

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      expectRpc(cache2, RemoveCommand.class);
      cache1.remove("key");
      waitForRpc(cache2);

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testPutIfAbsent() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "valueOld");
      assert cache2.get("key").equals("valueOld");
      assert cache1.get("key") == null;

      expectRpc(cache2, PutKeyValueCommand.class);
      cache1.putIfAbsent("key", "value");
      waitForRpc(cache2);

      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));

      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value3");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value3");

      cache1.putIfAbsent("key", "value4");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value3"); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.remove("key", "value");

      assert cache1.get("key").equals("value1") : "Should not remove";
      assert cache2.get("key").equals("value2") : "Should not remove";

      expectRpc(cache2, RemoveCommand.class);
      cache1.remove("key", "value1");
      waitForRpc(cache2);

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testClear() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      expectRpc(cache2, ClearCommand.class);
      cache1.clear();
      waitForRpc(cache2);

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testReplace() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      expectRpc(cache2, ReplaceCommand.class);
      cache1.replace("key", "value1");
      waitForRpc(cache2);

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }

   public void testReplaceWithOldVal() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key").equals("valueN");
      assert cache2.get("key").equals("value2");

      expectRpc(cache2, ReplaceCommand.class);
      cache1.replace("key", "valueN", "value1");
      waitForRpc(cache2);

      // the replace executed identically on both of them
      assertEquals("value1", cache1.get("key"));
      assertEquals("value1", cache2.get("key"));
   }

   public void testLocalOnlyClear() {
      AdvancedCache cache1 = cache(0,"replication").getAdvancedCache();
      AdvancedCache cache2 = cache(1,"replication").getAdvancedCache();
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).clear();

      assert cache1.get("key") == null;
      assert cache2.get("key") != null;
      assert cache2.get("key").equals("value2");
   }

   /*
    * Test helpers below
    */

   private void expectRpc(AdvancedCache cache, Class commandType) {
      if (!isSync) {
         replListener(cache).expect(commandType);
      }
   }

   private void waitForRpc(AdvancedCache cache) {
      if (!isSync) {
         replListener(cache).waitForRpc();
      }
   }
}
