package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "distribution.DistSyncL1PessimisticFuncTest")
public class DistSyncL1PessimisticFuncTest extends BaseDistFunctionalTest {

   public DistSyncL1PessimisticFuncTest() {
      sync = true;
      tx = true;
      testRetVals = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }

   public void testWriteLockBlockingForceWriteL1Update() throws Exception {
      final String key = "some-key";
      String value = "some-value";
      final String otherValue = "some-new-value";

      final Cache<Object, String> nonOwner = getFirstNonOwner(key);
      final Cache<Object, String> owner = getFirstOwner(key);

      owner.put(key, value);
      // Get put in L1
      nonOwner.get(key);

      assertIsInL1(nonOwner, key);

      try {
         // Owner now does a write
         TransactionManager ownerManger = TestingUtil.getTransactionManager(owner);
         ownerManger.begin();
         // This should lock the key
         owner.put(key, otherValue);

         // Now non owner tries to lock the key, but should get blocked
         Future<String> futureGet = fork(new Callable<String>() {

            @Override
            public String call() throws Exception {
               TransactionManager mgr = TestingUtil.getTransactionManager(nonOwner);
               mgr.begin();
               try {
                  return nonOwner.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);
               } finally {
                  mgr.commit();
               }
            }
         });

         // Get should not be able to complete
         try {
            futureGet.get(1, TimeUnit.SECONDS);
            fail("Get command should have blocked waiting");
         } catch (TimeoutException e) {

         }

         ownerManger.commit();

         assertEquals(otherValue, futureGet.get(1, TimeUnit.SECONDS));

         assertIsInL1(nonOwner, key);
      } finally {
         nonOwner.getAdvancedCache().removeInterceptor(BlockingInterceptor.class);
      }
   }

   public void testForceWriteLockWithL1Invalidation() throws Exception {
      final String key = "some-key";
      String value = "some-value";
      final String otherValue = "some-new-value";

      final Cache<Object, String> nonOwner = getFirstNonOwner(key);
      final Cache<Object, String> owner = getFirstOwner(key);

      owner.put(key, value);
      // Get put in L1
      nonOwner.get(key);

      assertIsInL1(nonOwner, key);

      try {
         // Owner now does a write
         TransactionManager ownerManger = TestingUtil.getTransactionManager(owner);
         ownerManger.begin();
         // This should lock the key
         assertEquals(value, nonOwner.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key));

         // Now non owner tries to lock the key, but should get blocked
         Future<String> futurePut = fork(new Callable<String>() {

            @Override
            public String call() throws Exception {
               TransactionManager mgr = TestingUtil.getTransactionManager(nonOwner);
               mgr.begin();
               try {
                  return owner.put(key, otherValue);
               } finally {
                  mgr.commit();
               }
            }
         });

         // Put should not be able to complete
         try {
            futurePut.get(1, TimeUnit.SECONDS);
            fail("Get command should have blocked waiting");
         } catch (TimeoutException e) {

         }

         ownerManger.commit();

         assertEquals(value, futurePut.get(1, TimeUnit.SECONDS));

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               // Value should be removed from L1 eventually
               return !isInL1(nonOwner, key);
            }
         });

         assertIsNotInL1(nonOwner, key);
      } finally {
         nonOwner.getAdvancedCache().removeInterceptor(BlockingInterceptor.class);
      }
   }
}
