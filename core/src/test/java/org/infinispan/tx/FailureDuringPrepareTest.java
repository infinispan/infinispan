/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.tx;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.FailureDuringPrepareTest")
@CleanupAfterMethod
public class FailureDuringPrepareTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      c.fluent().hash().numOwners(3);
      createCluster(c, 3);
      waitForClusterToForm();
   }

   public void testResourceCleanedIfPrepareFails() throws Exception {
      runTest(false);
   }

   public void testResourceCleanedIfPrepareFails2() throws Exception {
      runTest(true);
   }

   private void runTest(boolean multipleResources) throws NotSupportedException, SystemException, RollbackException {
      advancedCache(1).addInterceptor(new CommandInterceptor() {
         @Override
         public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
            try {
               return super.visitPrepareCommand(ctx, command);
            } finally {
               //allow the prepare to succeed then crash
               throw new RuntimeException("Induced fault!");
            }
         }
      },2);

      tm(0).begin();

      cache(0).put("k","v");

      if (multipleResources) {
         tm(0).getTransaction().enlistResource(new XAResourceAdapter());
      }

      assertEquals(lockManager(0).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(1).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(2).getNumberOfLocksHeld(), 0);

      try {
         tm(0).commit();
         assert false;
      } catch (Exception e) {
         e.printStackTrace();
      }

      assertEquals(lockManager(0).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(1).getNumberOfLocksHeld(), 0);
      assertEquals(lockManager(2).getNumberOfLocksHeld(), 0);
   }


}
