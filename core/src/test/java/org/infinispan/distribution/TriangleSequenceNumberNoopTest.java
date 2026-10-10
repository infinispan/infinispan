package org.infinispan.distribution;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commands.triangle.BackupNoopCommand;
import org.infinispan.commands.triangle.SingleKeyBackupWriteCommand;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledRpcManager.BlockedRequest;
import org.testng.annotations.Test;

/**
 * A sequence number taken by the primary owner for a backup write must always be followed by a backup write or a
 * {@link BackupNoopCommand}, also if the topology changes after the sequence number was taken.
 * Otherwise, the backups wait for it and block all later writes of the segment until they install a new topology.
 */
@Test(groups = "functional", testName = "distribution.TriangleSequenceNumberNoopTest")
public class TriangleSequenceNumberNoopTest extends MultipleCacheManagersTest {

   private static final int SEGMENT = 0;

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().marshaller(new JavaSerializationMarshaller());
      globalBuilder.serialization().allowList().addClasses(ControlledConsistentHashFactory.Default.class);

      // cache(0) is the primary owner and cache(1) the backup owner of the only segment
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numSegments(1);
      cacheBuilder.addModule(PrivateCacheConfigurationBuilder.class)
                  .consistentHashFactory(new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}}));
      createCluster(globalBuilder, cacheBuilder, 2);
      waitForClusterToForm();
   }

   public void testTopologyChangeAfterSequenceNumberTaken() throws Exception {
      int topologyId = extractComponent(cache(0), DistributionManager.class).getCacheTopology().getTopologyId();
      TriangleOrderManager primaryOrderManager = extractComponent(cache(0), TriangleOrderManager.class);

      // The first topology lookup after the first sequence number was taken sees a newer topology.
      // This simulates a topology installation between taking the sequence number and sending the backup write.
      AtomicBoolean armed = new AtomicBoolean();
      DistributionManager realDistributionManager = extractComponent(cache(0), DistributionManager.class);
      LocalizedCacheTopology newerTopology = mock(LocalizedCacheTopology.class);
      when(newerTopology.getTopologyId()).thenReturn(topologyId + 1);
      DistributionManager distributionManager = mock(DistributionManager.class,
            withSettings().defaultAnswer(delegatesTo(realDistributionManager)));
      doAnswer(invocation -> {
         if (armed.get() && primaryOrderManager.latestSent(SEGMENT, topologyId) >= 1 && armed.getAndSet(false)) {
            return newerTopology;
         }
         return realDistributionManager.getCacheTopology();
      }).when(distributionManager).getCacheTopology();
      replaceComponent(cache(0), DistributionManager.class, distributionManager, true);

      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(0));

      // register the waiters first, the commands are only blocked if somebody expects them
      CompletableFuture<BlockedRequest<BackupNoopCommand>> noopRequest =
            rpcManager.expectCommandAsync(BackupNoopCommand.class);

      armed.set(true);
      CompletableFuture<Object> firstWrite = cache(0).putAsync("key-1", "value-1");

      // the sequence number 1 was taken by the first write, the backup must be told to skip it
      BlockedRequest<BackupNoopCommand> noop = noopRequest.get(10, TimeUnit.SECONDS);
      assertEquals(1, noop.getCommand().getSequence());
      assertEquals(SEGMENT, noop.getCommand().getSegmentId());
      assertEquals(topologyId, noop.getCommand().getTopologyId());
      noop.send();

      // a later write of the same segment is not blocked by the missing sequence number
      CompletableFuture<BlockedRequest<SingleKeyBackupWriteCommand>> writeRequest =
            rpcManager.expectCommandAsync(SingleKeyBackupWriteCommand.class);
      CompletableFuture<Object> secondWrite = cache(0).putAsync("key-2", "value-2");
      BlockedRequest<SingleKeyBackupWriteCommand> write = writeRequest.get(10, TimeUnit.SECONDS);
      assertEquals(2, write.getCommand().getSequence());
      write.send();
      secondWrite.get(10, TimeUnit.SECONDS);

      assertEquals("value-2", cache(1).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get("key-2"));
      // the first write retries in the next topology, which is not installed in this test
      assertFalse(firstWrite.isDone());
   }
}
