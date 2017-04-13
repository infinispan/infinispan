package org.infinispan.distribution.rehash;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnOutboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.CommandMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Test that a joiner that became a backup owner for a key does not check the previous value when doing a conditional
 * write. Also check that if executing a write command during state transfer, it doesn't perform a remote get to obtain
 * the previous value from one of the readCH owners.
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxJoinerBecomingBackupOwnerTest")
@CleanupAfterMethod
public class NonTxJoinerBecomingBackupOwnerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testBackupOwnerJoiningDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE);
   }

   public void testBackupOwnerJoiningDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT);
   }

   public void testBackupOwnerJoiningDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE);
   }

   public void testBackupOwnerJoiningDuringReplaceWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT);
   }

   public void testBackupOwnerJoiningDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE);
   }

   public void testBackupOwnerJoiningDuringRemoveWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT);
   }

   protected void doTest(final TestWriteOperation op) throws Exception {
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("st", "st:cache0_before_send_state");
      sequencer.logicalThread("write", "write:before_start", "write:start", "write:cache1_before_return", "write:cache2_before_dist", "write:end", "write:after_end");
      sequencer.logicalThread("remote_get_cache0", "remote_get_cache0");
      sequencer.logicalThread("remote_get_cache1", "remote_get_cache1");
      sequencer.order("write:end", "remote_get_cache0").order("write:end", "remote_get_cache1");
      sequencer.action("st:cache0_before_send_state", () -> {
         sequencer.advance("write:before_start");
         // The whole write logical thread happens here
         sequencer.advance("write:after_end");
         return null;
      });

      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      final AdvancedCache<Object, Object> cache1 = advancedCache(1);

      // We only block the StateResponseCommand on cache0, because that's the node cache2 will ask for the magic key
      advanceOnOutboundRpc(sequencer, cache0, matchCommand(StateResponseCommand.class).build()).before("st:cache0_before_send_state");

      // Prohibit any remote get from cache2 to either cache0 or cache1
      advanceOnInterceptor(sequencer, cache0, StateTransferInterceptor.class, matchCommand(GetKeyValueCommand.class).build()).before("remote_get_cache0");
      advanceOnInterceptor(sequencer, cache1, StateTransferInterceptor.class, matchCommand(GetKeyValueCommand.class).build()).before("remote_get_cache1");

      // Add a new member, but don't start the cache yet
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);

      // Start the cache and wait until it's a member in the write CH
      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);

      // Wait for the write CH to contain the joiner everywhere
      eventually(() -> cache0.getRpcManager().getMembers().size() == 3 &&
            cache1.getRpcManager().getMembers().size() == 3 &&
            cache2.getRpcManager().getMembers().size() == 3);

      CommandMatcher writeCommandMatcher = matchCommand(op.getCommandClass()).build();
      // Allow the value to be written on cache1 before "write:cache1_before_return"
      advanceOnInterceptor(sequencer, cache1, StateTransferInterceptor.class, writeCommandMatcher).before("write:cache1_before_return");
      // The remote get (if any) will happen after "write:cache2_before_dist"
      advanceOnInterceptor(sequencer, cache2, StateTransferInterceptor.class, writeCommandMatcher).before("write:cache2_before_dist");

      // Wait for cache0 to send the StateResponseCommand to cache2, but keep it blocked
      sequencer.advance("write:start");

      final MagicKey key = getKeyForCache2();

      // Prepare for replace: put a previous value in cache0 and cache1
      if (op.getPreviousValue() != null) {
         cache0.withFlags(Flag.CACHE_MODE_LOCAL).put(key, op.getPreviousValue());
         cache1.withFlags(Flag.CACHE_MODE_LOCAL).put(key, op.getPreviousValue());
      }
      log.tracef("Initial value set, %s = %s", key, op.getPreviousValue());

      // Put from cache0 with cache0 as primary owner, cache2 will become a backup owner for the retry
      // The put command will be blocked on cache1 and cache2.
      Future<Object> future = fork(() -> op.perform(cache0, key));

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Allow the state transfer to finish, and any remote gets
      sequencer.advance("write:end");

      // Wait for the topology to change everywhere
      TestingUtil.waitForStableTopology(cache0, cache1, cache2);

      // Stop blocking get commands and check the value on all the nodes
      sequencer.stop();
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
      assertEquals(op.getValue(), cache2.get(key));

   }

   private MagicKey getKeyForCache2() {
      return new MagicKey(cache(0), cache(1), cache(2));
   }
}
