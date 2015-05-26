package org.infinispan.replication;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.InvokeRemotelyInFutureTest")
public class InvokeRemotelyInFutureTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      createClusteredCaches(2, "futureRepl", c);
   }

   public void testInvokeRemotelyInFutureWithListener() throws Exception {

      Cache cache1 = cache(0,"futureRepl");

      //never mind use of KeyValueCommand, the point is that callback is called and completes without Exceptions thrown
      final AtomicBoolean futureDoneOk = new AtomicBoolean();
      NotifyingFutureImpl<Map<Address, Response>> f = new NotifyingFutureImpl<>();
      f.attachListener(future -> {
         try {
            future.get();
            futureDoneOk.set(true);
         } catch (Exception e) {
            log.errorf(e, "Error reading future result");
         }
      });
      CommandsFactory cf = cache1.getAdvancedCache().getComponentRegistry().getComponent(CommandsFactory.class);
      PutKeyValueCommand put = cf.buildPutKeyValueCommand("k", "v",
            new EmbeddedMetadata.Builder().build(), null);
      cache1.getAdvancedCache().getRpcManager().invokeRemotelyInFuture(f, null,
            put, cache1.getAdvancedCache().getRpcManager().getDefaultRpcOptions(true));
      TestingUtil.sleepThread(2000);
      assert futureDoneOk.get();
   }
}
