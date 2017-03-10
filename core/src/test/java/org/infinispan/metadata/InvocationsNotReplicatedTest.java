package org.infinispan.metadata;

import static org.infinispan.util.ControlledRpcManager.replaceRpcManager;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledRpcManager.BlockedRequest;
import org.infinispan.util.ControlledRpcManager.BlockedResponse;
import org.infinispan.util.ControlledRpcManager.SentRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that {@link org.infinispan.commands.InvocationRecord} is not replicated when the entry is read remotely
 */
@Test(groups = "functional", testName = "metadata.InvocationsNotReplicatedTest")
public class InvocationsNotReplicatedTest extends MultipleCacheManagersTest {
   private ControlledRpcManager rpc;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      createClusteredCaches(3, cb);
   }

   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      rpc = replaceRpcManager(cache(2));
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      if (rpc != null) {
         rpc.revertRpcManager(cache(2));
      }
   }

   public void testGet() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));

      rpc.excludeCommands(PutKeyValueCommand.class);
      cache(2).put(key, "value");
      assertInvoked(cache(0), key);
      assertInvoked(cache(1), key);

      Future<Object> future = fork(() -> cache(2).get(key));
      verifyRemoteGets();
      assertEquals("value", future.get(30, TimeUnit.SECONDS));
   }

   public void testGetCacheEntry() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));

      rpc.excludeCommands(PutKeyValueCommand.class);
      cache(2).put(key, "value");
      assertInvoked(cache(0), key);
      assertInvoked(cache(1), key);

      Future<CacheEntry> future = fork(() -> cache(2).getAdvancedCache().getCacheEntry(key));
      verifyRemoteGets();
      CacheEntry entry = future.get(30, TimeUnit.SECONDS);
      assertEquals("value", entry.getValue());
      assertNotNull(entry.getMetadata());
   }

   public void testGetAll() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));

      rpc.excludeCommands(PutKeyValueCommand.class);
      cache(2).put(key, "value");
      assertInvoked(cache(0), key);
      assertInvoked(cache(1), key);

      Future<Map<Object, Object>> future = fork(() -> cache(2).getAdvancedCache().getAll(Collections.singleton(key)));
      verifyRemoteGetAlls();
      assertEquals("value", future.get(30, TimeUnit.SECONDS).values().iterator().next());
   }

   public void testGetAllCacheEntries() throws Exception {
      MagicKey key = new MagicKey(cache(0), cache(1));

      rpc.excludeCommands(PutKeyValueCommand.class);
      cache(2).put(key, "value");
      assertInvoked(cache(0), key);
      assertInvoked(cache(1), key);

      Future<Map<Object, CacheEntry<Object, Object>>> future = fork(() -> cache(2).getAdvancedCache().getAllCacheEntries(Collections.singleton(key)));
      verifyRemoteGetAlls();
      CacheEntry entry = future.get(30, TimeUnit.SECONDS).values().iterator().next();
      assertEquals("value", entry.getValue());
      assertNotNull(entry.getMetadata());
   }

   private void assertInvoked(Cache cache, MagicKey key) {
      InternalCacheEntry ice = cache.getAdvancedCache().getDataContainer().peek(key);
      assertNotNull(ice);
      assertNotNull(ice.getMetadata());
      InvocationRecord invocation = ice.getMetadata().lastInvocation();
      assertNotNull(invocation);
   }

   private void verifyRemoteGets() throws InterruptedException {
      BlockedRequest remoteGet = rpc.expectCommand(ClusteredGetCommand.class);
      SentRequest sentRequest = remoteGet.send();
      List<BlockedResponse> responses = new ArrayList<>();
      for (Address target : remoteGet.getTargets()) {
         responses.add(sentRequest.expectResponse(target, response -> {
            Object rspValue = ((ValidResponse) response).getResponseValue();
            InternalCacheValue icv = null;
            if (rspValue instanceof InternalCacheValue) {
               icv = (InternalCacheValue) rspValue;
            } else {
               fail("Value: " + rspValue);
            }
            assertNull(icv.getMetadata().lastInvocation());
         }));
      }
      responses.forEach(BlockedResponse::receive);
   }

   private void verifyRemoteGetAlls() throws InterruptedException {
      BlockedRequest remoteGet = rpc.expectCommand(ClusteredGetAllCommand.class);
      SentRequest sentRequest = remoteGet.send();
      List<BlockedResponse> responses = new ArrayList<>();
      for (Address target : remoteGet.getTargets()) {
         responses.add(sentRequest.expectResponse(target, response -> {
            Object rspValue = ((ValidResponse) response).getResponseValue();
            InternalCacheValue icv = null;
            if (rspValue instanceof InternalCacheValue[]) {
               icv = ((InternalCacheValue[]) rspValue)[0];
            } else {
               fail("Value: " + rspValue);
            }
            assertNull(icv.getMetadata().lastInvocation());
         }));
      }
      responses.forEach(BlockedResponse::receive);
   }
}
