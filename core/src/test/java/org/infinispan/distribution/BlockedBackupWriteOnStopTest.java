package org.infinispan.distribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractComponent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledRpcManager.BlockedRequest;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * A backup write that is not ready on a stopping cache (here: waiting for a lost, lower sequence number) must be
 * answered when the cache stops, instead of staying in the queue of the executor.
 */
@Test(groups = "functional", testName = "distribution.BlockedBackupWriteOnStopTest")
public class BlockedBackupWriteOnStopTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.serialization().marshaller(new JavaSerializationMarshaller());
      global.serialization().allowList().addClasses(ControlledConsistentHashFactory.Default.class);
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(60_000).hash().numSegments(1);
      config.addModule(PrivateCacheConfigurationBuilder.class)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}}));
      createCluster(global, config, 3);
      waitForClusterToForm();
   }

   public void testBackupWriteAnsweredWhenCacheStops() throws Exception {
      CommandAckCollector collector = Mockito.spy(extractComponent(cache(0), CommandAckCollector.class));
      TestingUtil.replaceComponent(cache(0), CommandAckCollector.class, collector, true);
      ControlledRpcManager primaryRpc = ControlledRpcManager.replaceRpcManager(cache(0));

      // The backup write with sequence 1 is lost, so the one with sequence 2 is not ready on the backup
      CompletableFuture<BlockedRequest<BackupWriteCommand>> first = primaryRpc.expectCommandAsync(BackupWriteCommand.class);
      CompletableFuture<Object> lost = cache(0).putAsync("lost", "v");
      first.get(10, TimeUnit.SECONDS).skipSend();

      CompletableFuture<BlockedRequest<BackupWriteCommand>> second = primaryRpc.expectCommandAsync(BackupWriteCommand.class);
      CompletableFuture<Object> blocked = cache(0).putAsync("blocked", "v");
      second.get(10, TimeUnit.SECONDS).send();
      primaryRpc.stopBlocking();

      Thread.sleep(500);
      assertThat(blocked).isNotDone();

      cache(1).stop();

      // The stopping backup rejects the blocked write, so that the originator retries in the next topology
      Mockito.verify(collector, Mockito.timeout(10_000))
            .completeExceptionally(Mockito.anyLong(), Mockito.argThat(t -> hasCause(t, OutdatedTopologyException.class)),
                  Mockito.anyInt());
      blocked.get(30, TimeUnit.SECONDS);
      lost.get(30, TimeUnit.SECONDS);
   }

   private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
      for (; t != null; t = t.getCause()) {
         if (type.isInstance(t)) return true;
      }
      return false;
   }
}
