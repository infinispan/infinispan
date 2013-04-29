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
package org.infinispan.tx;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * Test fpr ISPN-777.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.RemoteLockCleanupTest")
public class RemoteLockCleanupTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      super.createClusteredCaches(2, config);
   }

   public void testLockCleanup() throws Exception {
      final DelayInterceptor interceptor = new DelayInterceptor();
      advancedCache(0).addInterceptor(interceptor, 1);
      final Object k = getKeyForCache(0);

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               advancedCache(1).lock(k);
               tm(1).suspend();
            } catch (Exception e) {
               log.error(e);
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.receivedReplRequest;
         }
      });

      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0));
      TestingUtil.waitForRehashToComplete(cache(0));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return interceptor.lockAcquired;
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !TestingUtil.extractLockManager(cache(0)).isLocked("k");
         }
      });
   }


   static public class DelayInterceptor extends CommandInterceptor {

      volatile boolean receivedReplRequest = false;

      volatile boolean lockAcquired = false;


      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            receivedReplRequest = true;
            Thread.sleep(5000);
            try {
               return super.visitLockControlCommand(ctx, command);
            } finally {
               lockAcquired = true;
            }
         } else {

            return super.visitLockControlCommand(ctx, command);
         }
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         return super.visitRollbackCommand(ctx, command);
      }
   }
}
