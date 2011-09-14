/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.dld;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.Executor;

/**
 * Tests deadlock detection when t1 acquire (k1, k2) and te acquires (k2, k1).
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.dld.DldLazyLockingDistributionTest")
public class DldOptimisticLockingDistributionTest extends BaseDldOptimisticLockingTest {

   private KeyAffinityService cas;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = updatedConfig();
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(config);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      waitForClusterToForm();

      cas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0), new Executor() {
         public void execute(Runnable command) {
            new Thread(command).start();
         }
      }, new RndKeyGenerator(), 2, true);

      rpcManager0 = DldOptimisticLockingReplicationTest.replaceRpcManager(cache(0));
      rpcManager1 = DldOptimisticLockingReplicationTest.replaceRpcManager(cache(1));
   }

   protected Configuration updatedConfig() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setUnsafeUnreliableReturnValues(true);
      config.setNumOwners(1);
      config.setEnableDeadlockDetection(true);
      return config;
   }

   public void testSymmetricDeadlock() {
      Object k0 = cas.getKeyForAddress(address(0));
      Object k1 = cas.getKeyForAddress(address(1));
      testSymmetricDeadlock(k0, k1);
   }

   @AfterClass
   public void destroyKeyService() {
      cas.stop();
   }

}
