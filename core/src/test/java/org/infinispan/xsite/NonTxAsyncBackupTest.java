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

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.NonTxAsyncBackupTest")
public class NonTxAsyncBackupTest extends AbstractTwoSitesTest {

   private BlockingInterceptor blockingInterceptor;

   public NonTxAsyncBackupTest() {
      super.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
   }

   @Override
   protected void createSites() {
      super.createSites();
      blockingInterceptor = new BlockingInterceptor();
      backup("LON").getAdvancedCache().addInterceptor(blockingInterceptor, 1);
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @BeforeMethod
   void resetBlockingInterceptor() {
      blockingInterceptor.reset();
   }

   public void testPut() throws Exception {
      cache("LON", 0).put("k", "v");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache("LON", 0).get("k"));
      assertEquals("v", cache("LON", 1).get("k"));
      assertNull(backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return "v".equals(backup("LON").get("k"));
         }
      });
   }

   public void testRemove() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).remove("k");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return backup("LON").get("k") == null;
         }
      });
   }

   public void testClear() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).clear();
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return backup("LON").get("k") == null;
         }
      });
   }

   public void testReplace() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).replace("k", "v2");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v2", cache("LON", 0).get("k"));
      assertEquals("v2", cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return "v2".equals(backup("LON").get("k"));
         }
      });
   }

   public void testPutAll() throws Exception {
      cache("LON", 0).putAll(Collections.singletonMap("k", "v"));
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache("LON", 0).get("k"));
      assertEquals("v", cache("LON", 1).get("k"));
      assertNull(backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return "v".equals(backup("LON").get("k"));
         }
      });
   }

   private void doPutWithDisabledBlockingInterceptor() {
      blockingInterceptor.isActive = false;
      cache("LON", 0).put("k", "v");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return "v".equals(backup("LON").get("k"));
         }
      });
      blockingInterceptor.isActive = true;
   }

   public static class BlockingInterceptor extends CommandInterceptor {

      public volatile CountDownLatch invocationReceivedLatch = new CountDownLatch(1);

      public volatile CountDownLatch waitingLatch = new CountDownLatch(1);

      public volatile boolean isActive = true;

      void reset() {
         invocationReceivedLatch = new CountDownLatch(1);
         waitingLatch = new CountDownLatch(1);
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         return handle(ctx, command);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         return handle(ctx, command);
      }

      protected Object handle(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (isActive) {
            invocationReceivedLatch.countDown();
            waitingLatch.await();
         }
         return super.handleDefault(ctx, command);
      }
   }
}
