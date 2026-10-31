package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.OperationChannel;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests that the near cache automatically downgrades from primary-owner listener mode to a
 * cluster-wide listener when a server running an older Hot Rod protocol version joins the cluster
 * (simulating a rolling upgrade), and upgrades back to primary mode once all servers are on 4.2+.
 */
@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCacheRollingUpgradeTest")
public class PrimaryNearCacheRollingUpgradeTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.clustering().hash().numOwners(1);
      createHotRodServers(2, hotRodCacheConfiguration(cfg));
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      managers.forEach(RemoteCacheManager::stop);
      managers.clear();
      super.destroy();
   }

   private RemoteCacheManager createNearClient(boolean bloom) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100)
            .nearCacheUseBloomFilter(bloom);

      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      managers.add(rcm);
      return rcm;
   }

   public void testAutoDowngradeAndUpgradeOnRollingUpgrade() throws Exception {
      doTestAutoDowngradeAndUpgrade(false);
   }

   /**
    * Bloom filters rely on the listener channel in cluster mode, so the downgrade must track the channel
    * of the newly installed cluster listener.
    */
   public void testAutoDowngradeAndUpgradeOnRollingUpgradeWithBloomFilter() throws Exception {
      doTestAutoDowngradeAndUpgrade(true);
   }

   /**
    * Removing the near cache listener while downgraded must not allow a later codec change to re-install it.
    */
   public void testListenerRemovedWhileDowngradedIsNotReinstalled() throws Exception {
      RemoteCacheManager clientManager = createNearClient(false);
      RemoteCache<Integer, String> cache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      OperationDispatcher dispatcher = inrc.getDispatcher();
      OperationChannel targetChannel = targetChannel(inrc);
      Codec originalCodec = TestingUtil.extractField(targetChannel, "codec");
      Codec codec41 = org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_41.getCodec();
      TestingUtil.replaceField(codec41, "codec", targetChannel, OperationChannel.class);
      try {
         dispatcher.notifyCodecChanged();
         eventually(() -> !inrc.isPrimaryMode(), 5, TimeUnit.SECONDS);
         eventually(() -> inrc.getNearCacheService().isConnected(), 5, TimeUnit.SECONDS);

         Object nearListener = TestingUtil.extractField(inrc, "nearListener");
         assertThat(nearListener).isNotNull();
         inrc.removeClientListener(nearListener);
         assertThat((Object) TestingUtil.extractField(inrc, "nearListener")).isNull();

         // All servers are 4.2 again, but the application removed the listener so it must not come back
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
         dispatcher.notifyCodecChanged();
         Thread.sleep(500);
         assertThat(inrc.isPrimaryMode()).as("Removed listener must not be re-installed").isFalse();
         assertThat(inrc.getServerListenerStates()).isEmpty();
      } finally {
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
      }
   }

   private OperationChannel targetChannel(InvalidatedNearRemoteCache<Integer, String> inrc) {
      SocketAddress server0Addr = java.net.InetSocketAddress.createUnresolved(
            server(0).getHost(), server(0).getPort());
      OperationChannel targetChannel = inrc.getDispatcher().getHandlerForAddress(server0Addr);
      assertThat(targetChannel).as("OperationChannel for server 0").isNotNull();
      return targetChannel;
   }

   /**
    * Simulates the rolling-upgrade scenario:
    * <ol>
    *   <li>Start with a 2-node cluster — all servers at 4.2, near cache in primary mode.</li>
    *   <li>Simulate a server downgrade to 4.1 by replacing its codec to one that does not
    *       support primary near cache listeners and firing {@code notifyCodecChanged()}, while
    *       other threads concurrently read and write.</li>
    *   <li>Assert that the near cache downgrades to cluster-wide listener mode and clears.</li>
    *   <li>Simulate the server upgrading back to 4.2 by restoring the codec.</li>
    *   <li>Assert that the near cache upgrades back to primary mode.</li>
    * </ol>
    */
   private void doTestAutoDowngradeAndUpgrade(boolean bloom) throws Exception {
      RemoteCacheManager clientManager = createNearClient(bloom);
      RemoteCache<Integer, String> cache = clientManager.getCache();
      assertThat(cache).isInstanceOf(InvalidatedNearRemoteCache.class);
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;

      // --- Phase 1: all 4.2 — should be in primary mode ---
      assertThat(inrc.isPrimaryMode()).as("Initially in primary mode").isTrue();

      // Put and read a key to populate near cache
      cache.put(1, "one");
      assertThat(cache.get(1)).isEqualTo("one");
      assertThat(inrc.getNearCacheService().get(1)).as("Key cached in near cache").isNotNull();

      // --- Phase 2: simulate one server downgrading to 4.1 ---
      OperationDispatcher dispatcher = inrc.getDispatcher();
      OperationChannel targetChannel = targetChannel(inrc);
      Codec originalCodec = TestingUtil.extractField(targetChannel, "codec");
      Codec codec41 = org.infinispan.client.hotrod.ProtocolVersion.PROTOCOL_VERSION_41.getCodec();
      TestingUtil.replaceField(codec41, "codec", targetChannel, OperationChannel.class);

      // Operations running concurrently with the mode switches must neither fail nor observe stale values
      AtomicBoolean stop = new AtomicBoolean();
      Future<Void> concurrentOps = fork(() -> {
         int i = 0;
         while (!stop.get()) {
            int key = 100 + (i++ % 20);
            String value = "v" + i;
            cache.put(key, value);
            assertThat(cache.get(key)).isEqualTo(value);
         }
         return null;
      });
      try {
         // Fire the codec change notification as if the 4.1 server had just connected
         dispatcher.notifyCodecChanged();

         // The near cache should downgrade to cluster-wide listener mode and be functional again.
         eventually(() -> !inrc.isPrimaryMode(), 5, TimeUnit.SECONDS);
         assertThat(inrc.isPrimaryMode()).as("Should be in cluster mode after downgrade").isFalse();

         // Wait for the cluster listener to become connected and the near cache to be cleared.
         // switchToClusterListenerMode clears the near cache BEFORE installing the cluster listener,
         // so once isConnected() returns true the near cache is guaranteed to be clear.
         eventually(() -> inrc.getNearCacheService().isConnected() && inrc.getNearCacheService().get(1) == null,
               5, TimeUnit.SECONDS);

         if (bloom) {
            // Bloom filter updates and reads in cluster mode use the listener channel
            assertThat((Object) TestingUtil.extractField(inrc, "listenerChannel"))
                  .as("Cluster listener channel must be tracked after downgrade").isNotNull();
            inrc.updateBloomFilter().toCompletableFuture().get(10, TimeUnit.SECONDS);
         }

         // Cache is still functional in cluster-wide mode
         cache.put(2, "two");
         assertThat(cache.get(2)).isEqualTo("two");
         eventually(() -> inrc.getNearCacheService().get(2) != null, 5, TimeUnit.SECONDS);

         // --- Phase 3: simulate the server upgrading back to 4.2 ---
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
         dispatcher.notifyCodecChanged();

         // Near cache should upgrade back to primary mode
         eventually(() -> inrc.isPrimaryMode(), 5, TimeUnit.SECONDS);
         assertThat(inrc.isPrimaryMode()).as("Should be back in primary mode after upgrade").isTrue();

         stop.set(true);
         concurrentOps.get(10, TimeUnit.SECONDS);

         // Near cache is cleared during upgrade; reads should re-populate it
         cache.put(3, "three");
         assertThat(cache.get(3)).isEqualTo("three");
         eventually(() -> inrc.getNearCacheService().get(3) != null, 5, TimeUnit.SECONDS);

         // Invalidation via server still works in primary mode
         SocketAddress primaryOwner = inrc.getPrimaryOwner(3);
         assertThat(primaryOwner).as("Primary owner found").isNotNull();
         server(0).getCacheManager().getCache().put(3, "three-updated");
         eventually(() -> inrc.getNearCacheService().get(3) == null, 5, TimeUnit.SECONDS);
         assertThat(cache.get(3)).isEqualTo("three-updated");
      } finally {
         stop.set(true);
         // Restore original codec even if test fails
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
      }
   }

}
