package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.irac.DefaultIracTombstoneManager;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Tests the Scheduler in {@link DefaultIracTombstoneManager}.
 *
 * @author Pedro Ruivo
 * @since 14.0
 */
@Test(groups = "unit", testName = "xsite.irac.IracTombstoneUnitTest")
public class IracTombstoneUnitTest extends AbstractInfinispanTest {

   private static Configuration createConfiguration(int targetSize, long maxCleanupDelay) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.sites()
            .tombstoneMapSize(targetSize)
            .maxTombstoneCleanupDelay(maxCleanupDelay)
            .addBackup()
            .site("A")
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder.build();
   }


   private static DistributionManager createDistributionManager() {
      DistributionManager dm = Mockito.mock(DistributionManager.class);

      DistributionInfo dInfo = Mockito.mock(DistributionInfo.class);
      Mockito.when(dInfo.isPrimary()).thenReturn(true);
      Mockito.when(dInfo.isWriteOwner()).thenReturn(true);

      LocalizedCacheTopology cacheTopology = Mockito.mock(LocalizedCacheTopology.class);
      Mockito.when(cacheTopology.getSegmentDistribution(ArgumentMatchers.anyInt())).thenReturn(dInfo);

      Mockito.when(dm.getCacheTopology()).thenReturn(cacheTopology);
      return dm;
   }

   private static TakeOfflineManager createTakeOfflineManager() {
      TakeOfflineManager tom = Mockito.mock(TakeOfflineManager.class);
      // hack to prevent creating and mocking xsite commands. Local command needs to be mocked
      Mockito.when(tom.getSiteState(ArgumentMatchers.anyString())).thenReturn(SiteState.OFFLINE);
      return tom;
   }

   private static CommandsFactory createCommandFactory() {
      CommandsFactory factory = Mockito.mock(CommandsFactory.class);
      IracTombstoneRemoteSiteCheckCommand cmd = Mockito.mock(IracTombstoneRemoteSiteCheckCommand.class);
      Mockito.when(factory.buildIracTombstoneRemoteSiteCheckCommand(ArgumentMatchers.any())).thenReturn(cmd);

      IracTombstoneCleanupCommand cmd2 = Mockito.mock(IracTombstoneCleanupCommand.class);
      Mockito.when(cmd2.isEmpty()).thenReturn(false);
      Mockito.when(factory.buildIracTombstoneCleanupCommand(ArgumentMatchers.anyInt())).thenReturn(cmd2);

      return factory;
   }

   private static RpcManager createRpcManager() {
      RpcManager rpcManager = Mockito.mock(RpcManager.class);
      RpcOptions rpcOptions = Mockito.mock(RpcOptions.class);
      Transport transport = Mockito.mock(Transport.class);

      Mockito.doNothing().when(transport).checkCrossSiteAvailable();
      Mockito.when(transport.localSiteName()).thenReturn("B");
      Mockito.when(rpcManager.getTransport()).thenReturn(transport);
      Mockito.when(rpcManager.getSyncRpcOptions()).thenReturn(rpcOptions);
      Mockito.when(rpcManager.invokeCommand(ArgumentMatchers.anyCollection(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(CompletableFutures.completedNull());
      return rpcManager;
   }

   private static IracManager createIracManager(AtomicBoolean keep) {
      IracManager im = Mockito.mock(IracManager.class);
      Mockito.when(im.containsKey(ArgumentMatchers.any())).thenAnswer(invocationOnMock -> keep.get());
      return im;
   }

   private static ScheduledExecutorService createScheduledExecutorService(Queue<? super RunnableData> queue) {
      ScheduledExecutorService executorService = Mockito.mock(ScheduledExecutorService.class);
      Mockito.when(executorService.schedule(ArgumentMatchers.any(Runnable.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any())).thenAnswer(invocationOnMock -> {
         queue.add(new RunnableData(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)));
         return null;
      });
      return executorService;
   }

   private static IracMetadata createIracMetadata() {
      return Mockito.mock(IracMetadata.class);
   }

   private static BlockingManager createBlockingManager() {
      BlockingManager blockingManager = Mockito.mock(BlockingManager.class);
      Mockito.when(blockingManager.asExecutor(ArgumentMatchers.anyString())).thenReturn(new WithinThreadExecutor());
      Mockito.when(blockingManager.thenComposeBlocking(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenAnswer(invocationOnMock -> {
         CompletionStage<Void> stage = invocationOnMock.getArgument(0);
         Function<Void, CompletionStage<Object>> f = invocationOnMock.getArgument(1);
         return stage.thenCompose(f);
      });
      return blockingManager;
   }

   private static DefaultIracTombstoneManager createIracTombstoneManager(Queue<? super RunnableData> queue, int targetSize, long maxDelay, AtomicBoolean keep) {
      DefaultIracTombstoneManager manager = new DefaultIracTombstoneManager(createConfiguration(targetSize, maxDelay));
      TestingUtil.inject(manager,
            createDistributionManager(),
            createTakeOfflineManager(),
            createIracManager(keep),
            createBlockingManager(),
            createScheduledExecutorService(queue),
            createCommandFactory(),
            createRpcManager());
      return manager;
   }

   public void testDelayIncreaseWithNoTombstones() throws InterruptedException {
      BlockingDeque<RunnableData> queue = new LinkedBlockingDeque<>();
      DefaultIracTombstoneManager manager = createIracTombstoneManager(queue, 1, 1000, new AtomicBoolean(false));
      manager.start();

      RunnableData data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(500, data.delay);

      // check the max limit
      for (long expectedDelay : Arrays.asList(707, 841, 917, 958, 979, 989, 994, 997, 998, 999, 999, 999)) {
         data.runnable.run();
         data = queue.poll(10, TimeUnit.SECONDS);
         assertNotNull(data);
         assertEquals(expectedDelay, data.delay);
      }
      manager.stop();
   }

   public void testDelayAtSameRate() throws InterruptedException {
      int targetSize = 20;
      BlockingDeque<RunnableData> queue = new LinkedBlockingDeque<>();
      DefaultIracTombstoneManager manager = createIracTombstoneManager(queue, targetSize, 2000, new AtomicBoolean(false));
      manager.start();

      RunnableData data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1000, data.delay);

      IracMetadata metadata = createIracMetadata();
      insertTombstones(targetSize, manager, metadata);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1000, data.delay);
      manager.stop();
   }

   public void testDelayAtHigherRate() throws InterruptedException {
      int targetSize = 10;
      BlockingDeque<RunnableData> queue = new LinkedBlockingDeque<>();
      DefaultIracTombstoneManager manager = createIracTombstoneManager(queue, targetSize, 2000, new AtomicBoolean(false));
      manager.start();

      RunnableData data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1000, data.delay);

      IracMetadata metadata = createIracMetadata();
      insertTombstones(targetSize * 2, manager, metadata);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(708, data.delay);
   }

   public void testDelayAtLowerRate() throws InterruptedException {
      int targetSize = 20;
      BlockingDeque<RunnableData> queue = new LinkedBlockingDeque<>();
      DefaultIracTombstoneManager manager = createIracTombstoneManager(queue, targetSize, 2000, new AtomicBoolean(false));
      manager.start();

      RunnableData data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1000, data.delay);

      IracMetadata metadata = createIracMetadata();
      insertTombstones(targetSize / 2, manager, metadata);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1414, data.delay);
      manager.stop();
   }

   public void testCleanupCantKeepUp() throws InterruptedException {
      int targetSize = 5;
      BlockingDeque<RunnableData> queue = new LinkedBlockingDeque<>();
      AtomicBoolean keep = new AtomicBoolean(true);
      DefaultIracTombstoneManager manager = createIracTombstoneManager(queue, targetSize, 1000, keep);
      manager.start();

      RunnableData data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(500, data.delay);

      // Cleanup task didn't clean enough, delay goes down to 1ms
      IracMetadata metadata = createIracMetadata();
      insertTombstones(targetSize * 2, manager, metadata);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1, data.delay);

      // Cleanup task didn't clean enough again, delay stays at 1ms
      insertTombstones(targetSize * 3, manager, metadata);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(1, data.delay);

      // Tombstones are now cleaned up, and the delay goes back up
      keep.set(false);

      data.runnable.run();
      data = queue.poll(10, TimeUnit.SECONDS);
      assertNotNull(data);
      assertEquals(32, data.delay);

      manager.stop();
   }

   private static void insertTombstones(int targetSize, DefaultIracTombstoneManager manager, IracMetadata metadata) {
      for (int i = 0; i < targetSize; ++i) {
         manager.storeTombstone(1, i, metadata);
      }
   }

   private static final class RunnableData {
      final Runnable runnable;
      final long delay;

      private RunnableData(Runnable runnable, long delay) {
         this.runnable = runnable;
         this.delay = delay;
      }
   }
}
