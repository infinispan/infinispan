package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
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
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
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
      super.nycBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
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
      assertReachedRemoteSite();
      assertEquals("v", cache(LON, 0).get("k"));
      assertEquals("v", cache(LON, 1).get("k"));
      assertNull(backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals("v", () -> backup(LON).get("k"));
      assertDataContainerState("v");
      assertNoDataLeak();
   }

   public void testRemove() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).remove("k");
      assertReachedRemoteSite();
      assertNull(cache(LON, 0).get("k"));
      assertNull(cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals(null, () -> backup(LON).get("k"));
      assertNoDataLeak();
   }

   public void testClear() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).clear();
      assertReachedRemoteSite();
      assertNull(cache(LON, 0).get("k"));
      assertNull(cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals(null, () -> backup(LON).get("k"));
      assertNoDataLeak();
   }

   public void testReplace() throws Exception {
      doPutWithDisabledBlockingInterceptor();

      cache(LON, 1).replace("k", "v2");
      assertReachedRemoteSite();
      assertEquals("v2", cache(LON, 0).get("k"));
      assertEquals("v2", cache(LON, 1).get("k"));
      assertEquals("v", backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals("v2", () -> backup(LON).get("k"));
      assertDataContainerState("v2");
      assertNoDataLeak();
   }

   public void testPutAll() throws Exception {
      cache(LON, 0).putAll(Collections.singletonMap("k", "v"));
      assertReachedRemoteSite();
      assertEquals("v", cache(LON, 0).get("k"));
      assertEquals("v", cache(LON, 1).get("k"));
      assertNull(backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals("v", () -> backup(LON).get("k"));
      assertDataContainerState("v");
      assertNoDataLeak();
   }

   public void testPutForExternalRead() throws InterruptedException {
      cache(LON, 0).putForExternalRead("k", "v");
      assertReachedRemoteSite();
      // put for external read is async
      eventuallyEquals("v", () -> cache(LON, 0).get("k"));
      eventuallyEquals("v", () -> cache(LON, 1).get("k"));
      assertNull(backup(LON).get("k"));
      resumeRemoteSite();
      eventuallyEquals("v", () -> backup(LON).get("k"));
      assertDataContainerState("v");
      assertNoDataLeak();
   }

   private void doPutWithDisabledBlockingInterceptor() {
      blockingInterceptor.isActive = false;
      cache(LON, 0).put("k", "v");

      eventuallyEquals("v", () -> backup(LON).get("k"));
      blockingInterceptor.isActive = true;
   }

   private void assertReachedRemoteSite() throws InterruptedException {
      try {
         assertTrue(blockingInterceptor.invocationReceivedLatch.await(20000, TimeUnit.MILLISECONDS));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw e;
      }
   }

   private void resumeRemoteSite() {
      blockingInterceptor.waitingLatch.countDown();
   }

   private DistributionInfo getDistributionForKey(Cache<String, String> cache) {
      return extractComponent(cache, ClusteringDependentLogic.class)
            .getCacheTopology()
            .getDistribution("k");
   }

   private boolean isNotWriteOwner(Cache<String, String> cache) {
      return !getDistributionForKey(cache).isWriteOwner();
   }

   private Cache<String, String> findPrimaryOwner() {
      for (Cache<String, String> c : this.<String, String>caches(LON)) {
         if (getDistributionForKey(c).isPrimary()) {
            return c;
         }
      }
      throw new IllegalStateException(String.format("Unable to find primary owner for key %s", "k"));
   }

   private InternalDataContainer<String, String> getInternalDataContainer(Cache<String, String> cache) {
      //noinspection unchecked
      return extractComponent(cache, InternalDataContainer.class);
   }

   private IracMetadata extractMetadataFromPrimaryOwner() {
      Cache<String, String> cache = findPrimaryOwner();
      InternalDataContainer<String, String> dataContainer = getInternalDataContainer(cache);
      InternalCacheEntry<String, String> entry = dataContainer.peek("k");
      assertNotNull(entry);
      PrivateMetadata internalMetadata = entry.getInternalMetadata();
      assertNotNull(internalMetadata);
      IracMetadata metadata = internalMetadata.iracMetadata();
      assertNotNull(metadata);
      return metadata;
   }

   private void assertInDataContainer(String site, String value, IracMetadata metadata) {
      for (Cache<String, String> cache : this.<String, String>caches(site)) {
         if (isNotWriteOwner(cache)) {
            continue;
         }
         InternalDataContainer<String, String> dc = getInternalDataContainer(cache);
         InternalCacheEntry<String, String> ice = dc.peek("k");
         log.debugf("Checking DataContainer in %s. entry=%s", DistributionTestHelper.addressOf(cache), ice);
         assertNotNull(String.format("Internal entry is null for key %s", "k"), ice);
         assertEquals("Internal entry wrong key", "k", ice.getKey());
         assertEquals("Internal entry wrong value", value, ice.getValue());
         assertEquals("Internal entry wrong metadata", metadata, ice.getInternalMetadata().iracMetadata());
      }
   }

   private void assertDataContainerState(String value) {
      IracMetadata metadata = extractMetadataFromPrimaryOwner();
      assertInDataContainer(LON, value, metadata);
      assertInDataContainer(NYC, value, metadata);
   }

   private void assertNoDataLeak() {
      for (int  i = 0; i < initialClusterSize; ++i) {
         Cache<?,?> lonCache = cache(LON, null, i);
         Cache<?,?> nycCache = cache(NYC, null, i);
         eventually("Updated keys map is not empty in LON!", () -> isIracManagerEmpty(lonCache));
         eventually("Updated keys map is not empty in NYC!", () -> isIracManagerEmpty(nycCache));
         iracTombstoneManager(lonCache).startCleanupTombstone();
         iracTombstoneManager(nycCache).startCleanupTombstone();
      }
      for (int  i = 0; i < initialClusterSize; ++i) {
         Cache<?,?> lonCache = cache(LON, null, i);
         Cache<?,?> nycCache = cache(NYC, null, i);
         eventually("Tombstone map is not empty in LON", iracTombstoneManager(lonCache)::isEmpty);
         eventually("Tombstone map is not empty in NYC", iracTombstoneManager(nycCache)::isEmpty);
      }
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
            assertTrue(waitingLatch.await(30, TimeUnit.SECONDS));
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
