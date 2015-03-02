package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests that config option StateTransferConfiguration.awaitInitialTransfer() is honored correctly.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.InitialStateTransferCompletionTest")
@CleanupAfterMethod
public class InitialStateTransferCompletionTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder cacheConfigBuilder;

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheConfigBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      cacheConfigBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .syncCommitPhase(true).syncRollbackPhase(true)
            .lockingMode(LockingMode.PESSIMISTIC)
            .clustering().hash().numOwners(10)  // a number bigger than actual number of nodes will make this distributed cluster behave as if fully replicated
            .stateTransfer().fetchInMemoryState(true)
            .awaitInitialTransfer(true); // setting this to false will lead to test failure

      createCluster(cacheConfigBuilder, 2);
      waitForClusterToForm();
   }

   public void testStateTransferCompletion() throws Exception {
      final int numKeys = 100;

      // populate cache
      Cache<Object, Object> cache0 = cache(0);
      for (int i = 0; i < numKeys; i++) {
         cache0.put("k" + i, "v" + i);
      }

      final AtomicBoolean ignoreFurtherStateTransfer = new AtomicBoolean();
      final AtomicInteger transferredKeys = new AtomicInteger();
      cacheConfigBuilder.customInterceptors().addInterceptor().before(InvocationContextInterceptor.class).interceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
            if (cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               if (ignoreFurtherStateTransfer.get()) {
                  return null;
               }
               Object result = super.handleDefault(ctx, cmd);
               transferredKeys.incrementAndGet();
               return result;
            }

            return super.handleDefault(ctx, cmd);
         }
      });

      // add the third member
      log.trace("Adding new member ...");
      addClusterEnabledCacheManager(cacheConfigBuilder);
      Cache<String, String> cache2 = cache(2);  //this must return only when all state was received
      ignoreFurtherStateTransfer.set(true);
      log.trace("Successfully added a new member");

      // check number of transferred keys
      int actualTransferredKeys = transferredKeys.get();
      assertEquals(numKeys, actualTransferredKeys);

      // check the current topology
      CacheTopology cacheTopology = cache2.getAdvancedCache().getComponentRegistry().getStateTransferManager().getCacheTopology();
      assertEquals(cacheTopology.getWriteConsistentHash(), cacheTopology.getReadConsistentHash());
      ConsistentHash readCh = cacheTopology.getReadConsistentHash();
      assertTrue(readCh.getMembers().contains(address(2)));

      // check number of keys directly in data container
      DataContainer dc2 = cache(2).getAdvancedCache().getDataContainer();
      assertEquals(numKeys, dc2.size());

      // check the expected values of these keys
      for (int i = 0; i < numKeys; i++) {
         String key = "k" + i;
         String expectedValue = "v" + i;
         assertTrue(readCh.isKeyLocalToNode(address(2), key));
         InternalCacheEntry entry = dc2.get(key);
         assertNotNull(entry);
         assertEquals(expectedValue, entry.getValue());
      }
   }
}
