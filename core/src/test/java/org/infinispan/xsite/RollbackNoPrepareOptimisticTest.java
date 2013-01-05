/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.xsite;

import org.infinispan.Cache;
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
      BackupReceiver backupCacheManager = brr.getBackupCacheManager("LON", CacheContainer.DEFAULT_CACHE_NAME);
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

   public class BackupReceiverWrapper implements BackupReceiver {

      final BackupReceiver br;

      volatile VisitableCommand received;

      public BackupReceiverWrapper(BackupReceiver br) {
         this.br = br;
      }

      @Override
      public Cache getCache() {
         return br.getCache();
      }

      @Override
      public Object handleRemoteCommand(VisitableCommand command) throws Throwable {
         received = command;
         return br.handleRemoteCommand(command);
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
