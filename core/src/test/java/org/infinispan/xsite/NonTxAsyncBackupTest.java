package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.SiteMasterPickerImpl;
import org.infinispan.test.TestingUtil;
import org.jgroups.protocols.relay.RELAY2;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

   public void testSiteMasterPicked() throws NoSuchFieldException, IllegalAccessException {
      for (TestSite testSite : sites) {
         for (EmbeddedCacheManager cacheManager : testSite.cacheManagers) {
            RELAY2 relay2 = getRELAY2(cacheManager);
            Object site_master_picker = TestingUtil.extractField(RELAY2.class, relay2, "site_master_picker");
            assertEquals(SiteMasterPickerImpl.class, site_master_picker.getClass());
         }
      }
   }

   public void testPut() throws Exception {
      cache("LON", 0).put("k", "v");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache("LON", 0).get("k"));
      assertEquals("v", cache("LON", 1).get("k"));
      assertNull(backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v", () -> backup("LON").get("k"));
   }

   public void testRemove() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).remove("k");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals(null, () -> backup("LON").get("k"));
   }

   public void testClear() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).clear();
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals(null, () -> backup("LON").get("k"));
   }

   public void testReplace() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache("LON", 1).replace("k", "v2");
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v2", cache("LON", 0).get("k"));
      assertEquals("v2", cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v2", () -> backup("LON").get("k"));
   }

   public void testPutAll() throws Exception {
      cache("LON", 0).putAll(Collections.singletonMap("k", "v"));
      blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS);
      assertEquals("v", cache("LON", 0).get("k"));
      assertEquals("v", cache("LON", 1).get("k"));
      assertNull(backup("LON").get("k"));
      blockingInterceptor.waitingLatch.countDown();
      eventuallyEquals("v", () -> backup("LON").get("k"));
   }

   private void doPutWithDisabledBlockingInterceptor() {
      blockingInterceptor.isActive = false;
      cache("LON", 0).put("k", "v");

      eventuallyEquals("v", () -> backup("LON").get("k"));
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

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
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
