package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.ReplListener;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.HotRodAsyncReplicationTest")
public class HotRodAsyncReplicationTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false));
      builder.memory().maxCount(3);

      createHotRodServers(2, builder);
   }

   public void testPutKeyValue(Method m) {
      final RemoteCache<Object, Object> remoteCache0 = client(0).getCache();
      final RemoteCache<Object, Object> remoteCache1 = client(1).getCache();

      ReplListener replList0 = getReplListener(0);
      ReplListener replList1 = getReplListener(1);

      replList0.expect(PutKeyValueCommand.class);
      replList1.expect(PutKeyValueCommand.class);

      final String v1 = v(m);
      remoteCache0.put(1, v1);

      replList0.waitForRpc();
      replList1.waitForRpc();

      assertEquals(v1, remoteCache1.get(1));
      assertEquals(v1, remoteCache1.get(1)); // Called twice to cover all round robin options
      assertEquals(v1, remoteCache0.get(1));
      assertEquals(v1, remoteCache0.get(1)); // Called twice to cover all round robin options
   }

   private ReplListener getReplListener(int cacheIndex) {
      ReplListener replList = listeners.get(cache(cacheIndex));
      if (replList == null)
         replList = new ReplListener(cache(cacheIndex), true, true);
      else
         replList.reconfigureListener(true);

      return replList;
   }

}
