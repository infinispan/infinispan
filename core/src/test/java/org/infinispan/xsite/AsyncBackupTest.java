package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.AsyncBackupTest")
public class AsyncBackupTest extends AbstractTwoSitesTest {

   private BlockingInterceptor blockingInterceptor;

   private ConfigMode lonConfigMode;
   private ConfigMode nycConfigMode;

   private static ConfigurationBuilder getConfig(ConfigMode configMode) {
      if (configMode == ConfigMode.NON_TX) {
         return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      }
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      switch (configMode) {
         case OPTIMISTIC_TX_RC:
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
            break;
         case OPTIMISTIC_TX_RR:
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
            break;
         case PESSIMISTIC_TX:
            builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
            break;
      }
      return builder;
   }

   @Factory
   public Object[] factory() {
      List<AsyncBackupTest> tests = new LinkedList<>();
      for (ConfigMode lon : ConfigMode.values()) {
         for (ConfigMode nyc : ConfigMode.values()) {
            tests.add(new AsyncBackupTest().setLonConfigMode(lon).setNycConfigMode(nyc));
         }
      }
      return tests.toArray();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"LON", "NYC"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{lonConfigMode, nycConfigMode};
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getConfig(nycConfigMode);
   }

   public AsyncBackupTest() {
      super.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      super.implicitBackupCache = true;
   }

   @Override
   protected void createSites() {
      super.createSites();
      blockingInterceptor = new BlockingInterceptor();
      extractInterceptorChain(backup(LON)).addInterceptor(blockingInterceptor, 1);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getConfig(lonConfigMode);
   }

   private AsyncBackupTest setLonConfigMode(ConfigMode configMode) {
      this.lonConfigMode = configMode;
      return this;
   }

   private AsyncBackupTest setNycConfigMode(ConfigMode configMode) {
      this.nycConfigMode = configMode;
      return this;
   }

   @BeforeMethod
   void resetBlockingInterceptor() {
      blockingInterceptor.reset();
   }

   public void testPut() throws Exception {
      cache(LON, 0).put("k", "v");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache(LON, 0).get("k"));
      assertEquals("v", cache(LON, 1).get("k"));
      assertNull(backup(LON).get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v", () -> backup(LON).get("k"));
   }

   public void testRemove() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).remove("k");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache(LON, 0).get("k"));
      assertNull(cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals(null, () -> backup(LON).get("k"));
   }

   public void testClear() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).clear();
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache(LON, 0).get("k"));
      assertNull(cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals(null, () -> backup(LON).get("k"));
   }

   public void testReplace() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).replace("k", "v2");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v2", cache(LON, 0).get("k"));
      assertEquals("v2", cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v2", () -> backup(LON).get("k"));
   }

   public void testPutAll() throws Exception {
      cache(LON, 0).putAll(Collections.singletonMap("k", "v"));
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache(LON, 0).get("k"));
      assertEquals("v", cache(LON, 1).get("k"));
      assertNull(backup(LON).get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v", () -> backup(LON).get("k"));
   }

   private void doPutWithDisabledBlockingInterceptor() {
      blockingInterceptor.isActive = false;
      cache(LON, 0).put("k", "v");

      eventuallyEquals("v", () -> backup(LON).get("k"));
      blockingInterceptor.isActive = true;
   }

   public static class BlockingInterceptor extends DDAsyncInterceptor {

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
      public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command)
            throws Throwable {
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

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
         return handle(ctx, command);
      }

      protected Object handle(InvocationContext ctx, VisitableCommand command) throws Throwable {
         if (isActive) {
            invocationReceivedLatch.countDown();
            waitingLatch.await(30, TimeUnit.SECONDS);
         }
         return super.handleDefault(ctx, command);
      }
   }

   private enum ConfigMode {
      NON_TX,
      PESSIMISTIC_TX,
      OPTIMISTIC_TX_RC,
      OPTIMISTIC_TX_RR,
   }
}
