package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletionStage;

import javax.transaction.TransactionManager;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.RollbackNoPrepareOptimisticTest")
public class RollbackNoPrepareOptimisticTest extends AbstractTwoSitesTest {

   public RollbackNoPrepareOptimisticTest() {
      use2Pc = true;
   }

   public void testRollbackNoCommit() throws Throwable {
      String key = key(LON);
      String val = val(LON);

      ComponentRegistry cr = backup(LON).getAdvancedCache().getComponentRegistry();
      GlobalComponentRegistry gcr = cr.getGlobalComponentRegistry();
      BackupReceiverRepositoryImpl brr = (BackupReceiverRepositoryImpl) gcr.getComponent(BackupReceiverRepository.class);
      BackupReceiver backupCacheManager = brr.getBackupReceiver(LON, getDefaultCacheName());
      BackupReceiverWrapper brWrapper = new BackupReceiverWrapper(backupCacheManager);
      brr.replace(LON, getDefaultCacheName(), brWrapper);

      assertNull(brWrapper.received);
      cache(LON, 0).put(key, val);
      assertNotNull(brWrapper.received);
      assertEquals(backup(LON).get(key), val);

      brWrapper.received = null;

      TransactionManager tmLon0 = cache(LON, 0).getAdvancedCache().getTransactionManager();

      assertNull(brWrapper.received);
      tmLon0.begin();
      cache(LON, 0).put(key, val);
      log.trace("Before rollback!");
      tmLon0.rollback();
      assertNull(brWrapper.received);
   }

   public static class BackupReceiverWrapper extends BackupReceiverDelegator {

      volatile VisitableCommand received;

      BackupReceiverWrapper(BackupReceiver br) {
         super(br);
      }

      @Override
      public CompletionStage<Void> handleRemoteCommand(VisitableCommand command, boolean preserveOrder) {
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
