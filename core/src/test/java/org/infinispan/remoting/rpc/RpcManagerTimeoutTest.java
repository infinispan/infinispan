package org.infinispan.remoting.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.TimeoutException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.impl.FilterMapResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "remoting.rpc.RpcManagerTimeoutTest")
public class RpcManagerTimeoutTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "_cache_name_";

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithResponseFilter() {
      RpcManager rpcManager = advancedCache(0, CACHE_NAME).getRpcManager();
      final List<Address> members = rpcManager.getMembers();

      //wait for the responses from the last two members.
      ResponseFilter filter = new ResponseFilter() {

         private int expectedResponses = 2;

         @Override
         public boolean isAcceptable(Response response, Address sender) {
            if (sender.equals(members.get(2)) || sender.equals(members.get(3))) {
               expectedResponses--;
            }
            return true;
         }

         @Override
         public boolean needMoreResponses() {
            return expectedResponses > 0;
         }
      };

      doTest(new FilterMapResponseCollector(filter, true, 2), false);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithoutFilter() {
      doTest(null, false);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithBroadcast() {
      doTest(null, true);
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      createClusteredCaches(4, CACHE_NAME, RpcSCI.INSTANCE, builder);
      waitForClusterToForm(CACHE_NAME);
   }


   private void doTest(ResponseCollector<?> collector,boolean broadcast) {
      if (collector == null)
         collector = VoidResponseCollector.ignoreLeavers();

      RpcManager rpcManager = advancedCache(0, CACHE_NAME).getRpcManager();
      RpcOptions rpcOptions = new RpcOptions(DeliverOrder.NONE, 1000, TimeUnit.MILLISECONDS);
      CacheRpcCommand command = new SleepingCacheRpcCommand(ByteString.fromString(CACHE_NAME), 5000);
      if (broadcast) {
         rpcManager.blocking(rpcManager.invokeCommandOnAll(command, collector, rpcOptions));
      } else {
         List<Address> members = rpcManager.getMembers();
         ArrayList<Address> recipients = new ArrayList<>(2);
         recipients.add(members.get(2));
         recipients.add(members.get(3));
         rpcManager.blocking(rpcManager.invokeCommand(recipients, command, collector, rpcOptions));
      }
      Assert.fail("Timeout exception wasn't thrown");
   }
}
