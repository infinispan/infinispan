package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletionStage;

import jakarta.transaction.TransactionManager;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.RollbackNoPrepareOptimisticTest")
public class RollbackNoPrepareOptimisticTest extends AbstractTwoSitesTest {

   public RollbackNoPrepareOptimisticTest() {
      use2Pc = true;
   }

   public void testRollbackNoCommit() throws Throwable {
      String key = key(LON);
      String val = val(LON);

      LogBackupReceiver receiver = TestingUtil.wrapComponent(backup(LON), BackupReceiver.class, LogBackupReceiver::new);

      assertNull(receiver.received);
      cache(LON, 0).put(key, val);
      assertNotNull(receiver.received);
      assertEquals(backup(LON).get(key), val);

      receiver.received = null;

      TransactionManager tmLon0 = cache(LON, 0).getAdvancedCache().getTransactionManager();

      assertNull(receiver.received);
      tmLon0.begin();
      cache(LON, 0).put(key, val);
      log.trace("Before rollback!");
      tmLon0.rollback();
      assertNull(receiver.received);
   }

   public static class LogBackupReceiver extends BackupReceiverDelegator {

      volatile VisitableCommand received;

      protected LogBackupReceiver(BackupReceiver delegate) {
         super(delegate);
      }

      @Override
      public <O> CompletionStage<O> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
         received = command;
         return super.handleRemoteCommand(command, preserveOrder);
      }
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }
}
