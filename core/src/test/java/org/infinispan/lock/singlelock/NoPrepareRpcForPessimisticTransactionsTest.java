package org.infinispan.lock.singlelock;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.mocks.ControlledCommandFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.NoPrepareRpcForPessimisticTransactionsTest")
public class NoPrepareRpcForPessimisticTransactionsTest extends MultipleCacheManagersTest {

   private Object k1;
   private ControlledCommandFactory commandFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c
         .transaction().lockingMode(LockingMode.PESSIMISTIC)
         .clustering()
            .hash().numOwners(1)
            .l1().disable();
      createCluster(c, 2);
      waitForClusterToForm();

      k1 = getKeyForCache(1);
      commandFactory = ControlledCommandFactory.registerControlledCommandFactory(cache(1), CommitCommand.class);
   }

   @BeforeMethod
   void clearStats() {
      commandFactory.remoteCommandsReceived.set(0);
   }

   public void testSingleGetOnPut() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).put(k1, "v0");
         }
      };

      runtTest(o);
   }

   public void testSingleGetOnRemove() throws Exception {

      Operation o = new Operation() {
         @Override
         public void execute() {
            cache(0).remove(k1);
         }
      };

      runtTest(o);
   }

   private void runtTest(Operation o) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      log.trace("Here is where the fun starts..");
      tm(0).begin();

      o.execute();

      assertKeyLockedCorrectly(k1);

      assertEquals(commandFactory.remoteCommandsReceived.get(), 2, "2 = cluster get + lock");

      tm(0).commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            //prepare + tx completion notification
            return  commandFactory.remoteCommandsReceived.get()  == 4;
         }
      });
   }

   private interface Operation {
      void execute();
   }
}
