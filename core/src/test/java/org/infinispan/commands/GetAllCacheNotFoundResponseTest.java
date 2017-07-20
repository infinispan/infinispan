package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.GetAllCacheNotFoundResponseTest")
public class GetAllCacheNotFoundResponseTest extends MultipleCacheManagersTest {

   private MagicKey key1;
   private MagicKey key2;
   private MagicKey key3;
   private CountDownLatch cd1 = new CountDownLatch(2);
   private CountDownLatch cd2 = new CountDownLatch(3);
   private CompletableFuture<Map<Address, Response>> cf1 = new CompletableFuture<>();
   private CompletableFuture<Map<Address, Response>> cf2 = new CompletableFuture<>();
   private CompletableFuture<Map<Address, Response>> cf3 = new CompletableFuture<>();
   private CompletableFuture<Map<Address, Response>> cf4 = new CompletableFuture<>();
   private CompletableFuture<Map<Address, Response>> cf5 = new CompletableFuture<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      ControlledConsistentHashFactory.Default chf = new ControlledConsistentHashFactory.Default(
            new int[] { 0, 1 }, new int[] { 0, 2 }, new int[] { 2, 3 },
            new int[] { 1, 4 }, new int[] { 3, 4 }); // these are not important
      cb.clustering().hash().numOwners(2).numSegments(5).consistentHashFactory(chf);
      createClusteredCaches(5, cb);
   }

   public void test() throws InterruptedException, ExecutionException, TimeoutException {
      TestingUtil.wrapComponent(cache(4), RpcManager.class, FakeRpcManager::new);
      key1 = new MagicKey(cache(0), cache(1));
      key2 = new MagicKey(cache(0), cache(2));
      key3 = new MagicKey(cache(2), cache(3));

      // We expect the targets of ClusteredGetAllCommands to be selected in a specific way and for that we need
      // to iterate through the keys in certain order.
      Set<Object> keys = new LinkedHashSet<>(Arrays.asList(key1, key2, key3));
      Future<Map<Object, Object>> future = fork(() -> cache(4).getAdvancedCache().getAll(keys));

      // Wait until the first two ClusteredGetAllCommands are 'sent'
      assertTrue(cd1.await(10, TimeUnit.SECONDS));
      // Provide response for those commands
      cf1.complete(response(0, CacheNotFoundResponse.INSTANCE));
      cf2.complete(response(2, CacheNotFoundResponse.INSTANCE));

      // Wait until the second rounds is sent
      assertTrue(cd2.await(10, TimeUnit.SECONDS));
      // Provide a response for these commands.
      // We simulate that key1 is completely lost due to crashing nodes.
      cf3.complete(response(1, CacheNotFoundResponse.INSTANCE));
      cf4.complete(response(2, SuccessfulResponse.create(new InternalCacheValue[] { new ImmortalCacheValue("value2")})));
      cf5.complete(response(3, SuccessfulResponse.create(new InternalCacheValue[] { new ImmortalCacheValue("value3")})));

      Map<Object, Object> values = future.get(10, TimeUnit.SECONDS);
      // assertEquals is more verbose than assertNull in case of failure
      assertEquals(null, values.get(key1));
      assertEquals("value2", values.get(key2));
      assertEquals("value3", values.get(key3));
   }

   private Map<Address, Response> response(int target, Response response) {
      return Collections.singletonMap(address(cache(target)), response);
   }

   private class FakeRpcManager extends AbstractControlledRpcManager {

      public FakeRpcManager(RpcManager realOne) {
         super(realOne);
      }

      @Override
      public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options) {
         if (!(rpc instanceof ClusteredGetAllCommand)) {
            return super.invokeRemotelyAsync(recipients, rpc, options);
         }
         assertEquals(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, options.responseMode());
         ClusteredGetAllCommand cmd = (ClusteredGetAllCommand) rpc;
         if (hasKeys(cmd, key1, key2) && hasTarget(recipients, 0)) {
            cd1.countDown();
            return cf1;
         } else if (hasKeys(cmd, key3) && hasTarget(recipients, 2)) {
            cd1.countDown();
            return cf2;
         } else if (hasKeys(cmd, key1) && hasTarget(recipients, 1)) {
            cd2.countDown();
            return cf3;
         } else if (hasKeys(cmd, key2) && hasTarget(recipients, 2)) {
            cd2.countDown();
            return cf4;
         } else if (hasKeys(cmd, key3) && hasTarget(recipients, 3)) {
            cd2.countDown();
            return cf5;
         } else {
            throw new IllegalArgumentException("Command " + cmd + " to " + recipients);
         }
      }

      private boolean hasKeys(ClusteredGetAllCommand cmd, Object... keys) {
         if (cmd.getKeys().size() != keys.length) {
            return false;
         }
         for (Object key : keys) {
            if (!cmd.getKeys().contains(key)) {
               return false;
            }
         }
         return true;
      }

      private boolean hasTarget(Collection<Address> recipients, int target) {
         assertEquals(1, recipients.size());
         return recipients.iterator().next().equals(address(cache(target)));
      }

   }
}
