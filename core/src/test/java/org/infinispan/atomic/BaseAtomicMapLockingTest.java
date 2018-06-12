package org.infinispan.atomic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import javax.transaction.Transaction;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional")
public abstract class BaseAtomicMapLockingTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 3;
   private static final String VALUE = "value";
   private static final Object[] EMPTY_ARRAY = new Object[0];
   private final boolean pessimistic;
   private final CollectKeysInterceptor[] collectors = new CollectKeysInterceptor[NUM_NODES];
   private final Class<? extends PrepareCommand> prepareCommandClass;
   private Object ahmKey;
   private Object fgahmKey;

   protected BaseAtomicMapLockingTest(boolean pessimistic, Class<? extends PrepareCommand> prepareCommandClass) {
      this.pessimistic = pessimistic;
      this.prepareCommandClass = prepareCommandClass;
   }

   public final void testAtomicHasMapLockingOnLockOwner() throws Exception {
      testAtomicHashMap(true);
   }

   public final void testAtomicHasMapLockingOnNonLockOwner() throws Exception {
      testAtomicHashMap(false);
   }

   public final void testFineGrainedAtomicHashMapLockingOnLockOwner() throws Exception {
      testFineGrainedAtomicHashMap(true);
   }

   public final void testFineGrainedAtomicHashMapLockingOnNonLockOwner() throws Exception {
      testFineGrainedAtomicHashMap(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         collectors[i] = new CollectKeysInterceptor();
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
         builder.transaction().lockingMode(pessimistic ? LockingMode.PESSIMISTIC : LockingMode.OPTIMISTIC);
         builder.customInterceptors().addInterceptor().interceptor(collectors[i])
               .before(TxInterceptor.class);
         builder.clustering().hash().numOwners(2).groups().enabled();
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      ahmKey = new MagicKey("AtomicMap", cache(0));
      fgahmKey = new MagicKey("FineGrainedAtomicMap", cache(0));
   }

   protected final void testAtomicHashMap(boolean executeOnLockOwner) throws Exception {
      final int txExecutor = executeOnLockOwner ? 0 : 1;
      AtomicMap<Object, Object> map = AtomicMapLookup.getAtomicMap(cache(txExecutor), ahmKey);

      tm(txExecutor).begin();
      map.put("key1", VALUE);
      map.put("key2", VALUE);
      map.put("key3", VALUE);
      final Transaction tx1 = tm(txExecutor).suspend();

      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(txExecutor));
      try {
         Future<Boolean> txOutcome = fork(() -> {
            try {
               tm(txExecutor).resume(tx1);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               return Boolean.FALSE;
            }
         });

         ControlledRpcManager.BlockedRequest blockedPrepare = rpcManager.expectCommand(prepareCommandClass);
         ControlledRpcManager.BlockedResponseMap blockedPrepareResponses = null;
         if (!pessimistic) {
            blockedPrepareResponses = blockedPrepare.send().awaitAll();
         }

         assertKeysLocked(0, ahmKey);
         assertKeysLocked(1, EMPTY_ARRAY);
         assertKeysLocked(2, EMPTY_ARRAY);

         if (pessimistic) {
            blockedPrepare.send().receiveAll();
         } else {
            blockedPrepareResponses.receive();
            rpcManager.expectCommand(VersionedCommitCommand.class).send().receiveAll();
         }

         rpcManager.expectCommand(TxCompletionNotificationCommand.class).sendWithoutResponses();
         Assert.assertTrue(txOutcome.get());
      } finally {
         rpcManager.revertRpcManager();
      }
   }

   protected final void testFineGrainedAtomicHashMap(boolean executeOnLockOwner) throws Exception {
      final int txExecutor = executeOnLockOwner ? 0 : 1;
      FineGrainedAtomicMap<Object, Object> map = AtomicMapLookup.getFineGrainedAtomicMap(cache(txExecutor), fgahmKey);

      tm(txExecutor).begin();
      for (int keyIndex = 0; keyIndex < 100; ++keyIndex) {
         map.put("key" + keyIndex, VALUE);
      }
      final Transaction tx1 = tm(txExecutor).suspend();

      Assert.assertEquals(collectors[txExecutor].getKeys().size(), 100,
            "Wrong number of composite keys collected!");
      log.infof("%s composite keys collected.", collectors[txExecutor].getKeys().size());

      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(txExecutor));
      try {
         Future<Boolean> txOutcome = fork(() -> {
            try {
               tm(txExecutor).resume(tx1);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               return Boolean.FALSE;
            }
         });

         ControlledRpcManager.BlockedRequest blockedPrepare = rpcManager.expectCommand(prepareCommandClass);
         ControlledRpcManager.BlockedResponseMap blockedPrepareResponses = null;
         if (!pessimistic) {
            blockedPrepareResponses = blockedPrepare.send().awaitAll();
         }

         assertKeysLocked(0, collectors[txExecutor].getKeys().toArray());
         assertKeysLocked(1, EMPTY_ARRAY);
         assertKeysLocked(2, EMPTY_ARRAY);

         if (pessimistic) {
            blockedPrepare.send().receiveAll();
         } else {
            blockedPrepareResponses.receive();
            rpcManager.expectCommand(VersionedCommitCommand.class).send().receiveAll();
         }

         rpcManager.expectCommand(TxCompletionNotificationCommand.class).sendWithoutResponses();
         Assert.assertTrue(txOutcome.get());
      } finally {
         rpcManager.revertRpcManager();
      }
   }

   protected void assertKeysLocked(int index, Object... keys) {
      LockManager lockManager = lockManager(index);
      Assert.assertNotNull(keys);
      for (Object key : keys) {
         Assert.assertTrue(lockManager.isLocked(key), key + " is not locked in cache(" + index + ").");
      }
   }

   @AfterMethod(alwaysRun = true)
   public void afterMethod() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         if (collectors[i] != null) {
            collectors[i].reset();
         }
      }
   }

   private static class CollectKeysInterceptor extends BaseCustomInterceptor {

      private final Set<Object> keys;

      public CollectKeysInterceptor() {
         keys = new HashSet<>();
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         synchronized (keys) {
            keys.addAll(Arrays.asList(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      }

      public final void reset() {
         synchronized (keys) {
            keys.clear();
         }
      }

      public final Collection<Object> getKeys() {
         synchronized (keys) {
            return new ArrayList<>(keys);
         }
      }
   }
}
