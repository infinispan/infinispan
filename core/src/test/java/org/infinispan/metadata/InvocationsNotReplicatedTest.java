package org.infinispan.metadata;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractControlledRpcManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests that {@link org.infinispan.commands.InvocationRecord} is not replicated when the entry is read remotely
 */
@Test(groups = "functional", testName = "metadata.InvocationsNotReplicatedTest")
public class InvocationsNotReplicatedTest extends MultipleCacheManagersTest {
   private CheckingRpcManager checkingRpcManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      createClusteredCaches(3, cb);
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      if (checkingRpcManager != null) {
         checkingRpcManager.uninstall();
      }
   }

   public void testGet() {
      MagicKey key = new MagicKey(cache(0), cache(1));

      cache(2).put(key, "value");
      assertInvoked(cache(0), key, true);
      assertInvoked(cache(1), key, false);

      checkingRpcManager = CheckingRpcManager.install(cache(2));

      assertEquals("value", cache(2).get(key));
      checkingRpcManager.assertPositiveResponses();
   }

   public void testGetCacheEntry() {
      MagicKey key = new MagicKey(cache(0), cache(1));

      cache(2).put(key, "value");
      assertInvoked(cache(0), key, true);
      assertInvoked(cache(1), key, false);

      checkingRpcManager = CheckingRpcManager.install(cache(2));

      CacheEntry entry = cache(2).getAdvancedCache().getCacheEntry(key);
      assertEquals("value", entry.getValue());
      assertNotNull(entry.getMetadata());
      checkingRpcManager.assertPositiveResponses();
   }

   public void testGetAll() {
      MagicKey key = new MagicKey(cache(0), cache(1));

      cache(2).put(key, "value");
      assertInvoked(cache(0), key, true);
      assertInvoked(cache(1), key, false);

      checkingRpcManager = CheckingRpcManager.install(cache(2));

      Map<Object, Object> all = cache(2).getAdvancedCache().getAll(Collections.singleton(key));
      assertEquals("value", all.values().iterator().next());
      checkingRpcManager.assertPositiveResponses();
   }


   @Test(enabled = false, description = "Fails due to ISPN-7610")
   public void testGetAllCacheEntries() {
      MagicKey key = new MagicKey(cache(0), cache(1));

      cache(2).put(key, "value");
      assertInvoked(cache(0), key, true);
      assertInvoked(cache(1), key, false);

      checkingRpcManager = CheckingRpcManager.install(cache(2));

      Map<Object, CacheEntry<Object, Object>> all = cache(2).getAdvancedCache().getAllCacheEntries(Collections.singleton(key));
      CacheEntry entry = all.values().iterator().next();
      assertEquals("value", entry.getValue());
      assertNotNull(entry.getMetadata());
      checkingRpcManager.assertPositiveResponses();
   }

   private void assertInvoked(Cache cache, MagicKey key, boolean authoritative) {
      InternalCacheEntry ice = cache.getAdvancedCache().getDataContainer().peek(key);
      assertNotNull(ice);
      assertNotNull(ice.getMetadata());
      InvocationRecord invocation = ice.getMetadata().lastInvocation();
      assertNotNull(invocation);
      assertEquals(authoritative, invocation.isAuthoritative());
   }

   private static class CheckingRpcManager extends AbstractControlledRpcManager {
      int responsesChecked = 0;
      Cache cache;

      private static CheckingRpcManager install(Cache cache) {
         RpcManager original = TestingUtil.extractComponent(cache, RpcManager.class);
         CheckingRpcManager checkingRpcManager = new CheckingRpcManager(cache, original);
         TestingUtil.replaceComponent(cache, RpcManager.class, checkingRpcManager, true);
         return checkingRpcManager;
      }

      public CheckingRpcManager(Cache cache, RpcManager realOne) {
         super(realOne);
         this.cache = cache;
      }

      @Override
      protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap, Object argument) {
         for (Response response : responseMap.values()) {
            if (response.isValid()) {
               Object rspValue = ((ValidResponse) response).getResponseValue();
               InternalCacheValue icv = null;
               if (rspValue instanceof InternalCacheValue) {
                  icv = (InternalCacheValue) rspValue;
               } else if (rspValue instanceof InternalCacheValue[]) {
                  icv = ((InternalCacheValue[]) rspValue)[0];
               } else {
                  fail("Value: " + rspValue);
               }
               assertNull(icv.getMetadata().lastInvocation());
               ++responsesChecked;
            }
         }
         return responseMap;
      }

      public void uninstall() {
         TestingUtil.replaceComponent(cache, RpcManager.class, realOne, true);
      }

      public void assertPositiveResponses() {
         assertTrue("Too few responses: " + responsesChecked, responsesChecked > 0);
      }
   }

}
