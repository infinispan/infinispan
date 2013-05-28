/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.atomic;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

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
   private final CollectCompositeKeysInterceptor[] collectors = new CollectCompositeKeysInterceptor[NUM_NODES];
   private final ControlledRpcManager[] rpcManagers = new ControlledRpcManager[NUM_NODES];
   private Object ahmKey;
   private Object fgahmKey;

   protected BaseAtomicMapLockingTest(boolean pessimistic) {
      this.pessimistic = pessimistic;
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
         collectors[i] = new CollectCompositeKeysInterceptor();
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
         builder.transaction().lockingMode(pessimistic ? LockingMode.PESSIMISTIC : LockingMode.OPTIMISTIC);
         builder.customInterceptors().addInterceptor().interceptor(collectors[i])
               .before(TxInterceptor.class);
         builder.clustering().hash().numOwners(2);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      for (int i = 0; i < NUM_NODES; ++i) {
         RpcManager rpcManager = TestingUtil.extractComponent(cache(i), RpcManager.class);
         rpcManagers[i] = new ControlledRpcManager(rpcManager);
         TestingUtil.replaceComponent(cache(i), RpcManager.class, rpcManagers[i], true);
      }
      ahmKey = new MagicKey("AtomicHashMap", cache(0));
      fgahmKey = new MagicKey("FineGrainedAtomicHashMap", cache(0));
   }

   protected final void testAtomicHashMap(boolean executeOnLockOwner) throws Exception {
      resetBeforeMethod();
      final int txExecutor = executeOnLockOwner ? 0 : 1;
      AtomicMap<Object, Object> map = AtomicMapLookup.getAtomicMap(cache(txExecutor), ahmKey);

      tm(txExecutor).begin();
      map.put("key1", VALUE);
      map.put("key2", VALUE);
      map.put("key3", VALUE);
      final Transaction tx1 = tm(txExecutor).suspend();

      if (pessimistic) {
         rpcManagers[txExecutor].blockBefore(PrepareCommand.class);
      } else {
         rpcManagers[txExecutor].blockAfter(PrepareCommand.class);
      }

      Future<Boolean> txOutcome = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               tm(txExecutor).resume(tx1);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               return Boolean.FALSE;
            }
         }
      });

      try {
         rpcManagers[txExecutor].waitForCommandToBlock();
         assertKeysLocked(0, ahmKey);
         assertKeysLocked(1, EMPTY_ARRAY);
         assertKeysLocked(2, EMPTY_ARRAY);

         rpcManagers[txExecutor].stopBlocking();
         Assert.assertTrue(txOutcome.get());
      } finally {
         rpcManagers[txExecutor].stopBlocking();
      }
   }

   protected final void testFineGrainedAtomicHashMap(boolean executeOnLockOwner) throws Exception {
      resetBeforeMethod();
      final int txExecutor = executeOnLockOwner ? 0 : 1;
      FineGrainedAtomicMap<Object, Object> map = AtomicMapLookup.getFineGrainedAtomicMap(cache(txExecutor), fgahmKey);

      boolean hasLocalKeys = false;
      boolean hasRemoteKeys = false;
      int keyIndex = 0;

      tm(txExecutor).begin();
      while (!hasLocalKeys || !hasRemoteKeys) {
         map.put("key" + keyIndex++, VALUE);
         //has composite keys mapped to the lock owner?
         hasLocalKeys = hasKeyMappedTo(true, collectors[txExecutor].getCompositeKeys());
         //has composite keys mapped to the non lock owner?
         hasRemoteKeys = hasKeyMappedTo(false, collectors[txExecutor].getCompositeKeys());
         //the locks are independent on where the composite keys are mapped to.
      }
      final Transaction tx1 = tm(txExecutor).suspend();

      Assert.assertEquals(collectors[txExecutor].getCompositeKeys().size(), keyIndex,
                          "Wrong number of composite keys collected!");
      log.infof("%s composite keys collected.", collectors[txExecutor].getCompositeKeys().size());

      if (pessimistic) {
         rpcManagers[txExecutor].blockBefore(PrepareCommand.class);
      } else {
         rpcManagers[txExecutor].blockAfter(PrepareCommand.class);
      }

      Future<Boolean> txOutcome = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               tm(txExecutor).resume(tx1);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               return Boolean.FALSE;
            }
         }
      });

      try {
         rpcManagers[txExecutor].waitForCommandToBlock();
         assertKeysLocked(0, collectors[txExecutor].getCompositeKeys().toArray());
         assertKeysLocked(1, EMPTY_ARRAY);
         assertKeysLocked(2, EMPTY_ARRAY);

         rpcManagers[txExecutor].stopBlocking();
         Assert.assertTrue(txOutcome.get());
      } finally {
         rpcManagers[txExecutor].stopBlocking();
      }
   }

   protected void assertKeysLocked(int index, Object... keys) {
      LockManager lockManager = lockManager(index);
      Assert.assertNotNull(keys);
      for (Object key : keys) {
         Assert.assertTrue(lockManager.isLocked(key), key + " is not locked in cache(" + index + ").");
      }
   }

   protected boolean hasKeyMappedTo(boolean toLockOwner, Collection<Object> keys) {
      for (Object key : keys) {
         boolean onLockOwner = DistributionTestHelper.isFirstOwner(cache(0), key);
         if ((toLockOwner && onLockOwner) || (!toLockOwner && !onLockOwner)) {
            return true;
         }
      }
      return false;
   }

   private void resetBeforeMethod() {
      for (int i = 0; i < NUM_NODES; ++i) {
         if (collectors[i] != null) {
            collectors[i].reset();
         }
         if (rpcManagers[i] != null) {
            rpcManagers[i].stopBlocking();
            rpcManagers[i].stopFailing();
         }
      }
   }

   private static class CollectCompositeKeysInterceptor extends BaseCustomInterceptor {

      private final Set<Object> compositeKeys;

      public CollectCompositeKeysInterceptor() {
         compositeKeys = new HashSet<Object>();
      }

      @Override
      public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
         synchronized (compositeKeys) {
            compositeKeys.addAll(Arrays.asList(command.getCompositeKeys()));
         }
         return invokeNextInterceptor(ctx, command);
      }

      public final void reset() {
         synchronized (compositeKeys) {
            compositeKeys.clear();
         }
      }

      public final Collection<Object> getCompositeKeys() {
         synchronized (compositeKeys) {
            return new ArrayList<Object>(compositeKeys);
         }
      }
   }
}
