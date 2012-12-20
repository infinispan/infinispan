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

package org.infinispan.xsite.backupfailure;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite")
public abstract class BaseBackupFailureTest extends AbstractTwoSitesTest {

   protected FailureInterceptor failureInterceptor;

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new FailureInterceptor();
      backup("LON").getAdvancedCache().addInterceptor(failureInterceptor, 1);
   }

   @BeforeMethod
   void resetFailureInterceptor() {
      failureInterceptor.reset();
   }
   
   public static class FailureInterceptor extends CommandInterceptor {
      
      protected volatile boolean isFailing = true;

      protected volatile boolean rollbackFailed;
      protected volatile boolean commitFailed;
      protected volatile boolean prepareFailed;
      protected volatile boolean putFailed;
      protected volatile boolean removeFailed;
      protected volatile boolean replaceFailed;
      protected volatile boolean clearFailed;
      protected volatile boolean putMapFailed;

      protected volatile boolean dontFailPrepare;

      public void reset() {
         rollbackFailed = false;
         commitFailed = false;
         prepareFailed = false;
         putFailed = false;
         removeFailed = false;
         removeFailed = false;
         clearFailed = false;
         putMapFailed = false;
         dontFailPrepare = false;
         isFailing = true;
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         if (isFailing) {
            rollbackFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (isFailing) {
            commitFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (isFailing && !dontFailPrepare) {
            prepareFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (isFailing) {
            putFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         if (isFailing) {
            removeFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         if (isFailing) {
            replaceFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         if (isFailing) {
            clearFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         if (isFailing ) {
            putMapFailed = true;
            throw new CacheException("Induced failure");
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      public void disable() {
         isFailing = false;
      }

      public void enable() {
         isFailing = true;
      }

      public void dontFailPrepare() {
         dontFailPrepare = true;
      }
   }

   protected boolean failOnBackupFailure(String site, int cacheIndex) {
      return cache(site, cacheIndex).getCacheConfiguration().sites().allBackups().get(0).backupFailurePolicy() == BackupFailurePolicy.FAIL;
   }
}
