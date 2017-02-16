package org.infinispan.remoting.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.TimeoutException;
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

      doTest(filter, false, false);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithoutFilter() {
      doTest(null, false, false);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithBroadcast() {
      doTest(null, false, true);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithTotalOrderBroadcast() {
      doTest(null, true, true);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutWithTotalOrderAnycast() {
      doTest(null, true, false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      createClusteredCaches(4, CACHE_NAME, builder);
      waitForClusterToForm(CACHE_NAME);
   }

   private void doTest(ResponseFilter filter, boolean totalOrder, boolean broadcast) {
      RpcManager rpcManager = advancedCache(0, CACHE_NAME).getRpcManager();
      RpcOptionsBuilder builder = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS, totalOrder ? DeliverOrder.TOTAL : DeliverOrder.NONE)
            .timeout(1000, TimeUnit.MILLISECONDS);
      ArrayList<Address> recipients = null;
      if (!broadcast) {
         List<Address> members = rpcManager.getMembers();
         recipients = new ArrayList<>(2);
         recipients.add(members.get(2));
         recipients.add(members.get(3));
      }
      if (filter != null) {
         builder.responseFilter(filter);
      }
      rpcManager.invokeRemotely(recipients, new SleepingCacheRpcCommand(ByteString.fromString(CACHE_NAME), 5000), builder.build());
      Assert.fail("Timeout exception wasn't thrown");
   }


}
