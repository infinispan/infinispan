package org.infinispan.xsite;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.CacheContainer;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "xsite", testName = "xsite.RollbackNoPrepareOptimisticTest")
public class RollbackNoPrepareOptimisticTest extends AbstractTwoSitesTest {

   public RollbackNoPrepareOptimisticTest() {
      use2Pc = true;
   }

   public void testRollbackNoCommit() throws Throwable {
      String key = key("LON");
      String val = val("LON");

      ComponentRegistry cr = backup("LON").getAdvancedCache().getComponentRegistry();
      GlobalComponentRegistry gcr = cr.getGlobalComponentRegistry();
      BackupReceiverRepositoryImpl brr = (BackupReceiverRepositoryImpl) gcr.getComponent(BackupReceiverRepository.class);
      BackupReceiver backupCacheManager = brr.getBackupReceiver("LON", CacheContainer.DEFAULT_CACHE_NAME);
      BackupReceiverWrapper brWrapper = new BackupReceiverWrapper(backupCacheManager);
      brr.replace("LON", CacheContainer.DEFAULT_CACHE_NAME, brWrapper);

      assertNull(brWrapper.received);
      cache("LON", 0).put(key, val);
      assertNotNull(brWrapper.received);
      assertEquals(backup("LON").get(key), val);

      brWrapper.received = null;

      TransactionManager tmLon0 = cache("LON", 0).getAdvancedCache().getTransactionManager();

      assertNull(brWrapper.received);
      tmLon0.begin();
      cache("LON", 0).put(key, val);
      log.trace("Before rollback!");
      tmLon0.rollback();
      assertNull(brWrapper.received);
   }

   public class BackupReceiverWrapper extends BackupReceiverDelegator {

      volatile VisitableCommand received;

      public BackupReceiverWrapper(BackupReceiver br) {
         super(br);
      }

      @Override
      public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
         received = command;
         return super.handleRemoteCommand(command);
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
