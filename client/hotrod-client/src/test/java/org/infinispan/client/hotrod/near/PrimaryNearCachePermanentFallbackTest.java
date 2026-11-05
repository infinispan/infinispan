package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests the permanent-disable path ({@code cleanupPrimaryMode}) of primary-owner near cache mode: invoking
 * {@code cleanupPrimaryMode()} sets {@code primaryModeDisabled} permanently and leaves the near cache
 * in cluster-wide mode. Subsequent codec-change notifications must not re-enter primary mode.
 */
@Test(groups = "functional", testName = "client.hotrod.near.PrimaryNearCachePermanentFallbackTest")
public class PrimaryNearCachePermanentFallbackTest extends MultiHotRodServersTest {

   private final List<RemoteCacheManager> managers = new ArrayList<>();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      managers.forEach(RemoteCacheManager::stop);
      managers.clear();
      super.destroy();
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   private RemoteCacheManager createNearClient(ClientIntelligence intelligence) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      for (HotRodServer server : servers)
         clientBuilder.addServer().host("127.0.0.1").port(server.getPort());

      clientBuilder.clientIntelligence(intelligence);
      clientBuilder.remoteCache("")
            .nearCacheMode(NearCacheMode.INVALIDATED)
            .nearCacheMaxEntries(100);

      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      managers.add(rcm);
      return rcm;
   }

   /**
    * Verifies {@code cleanupPrimaryMode()} semantics: after calling it,
    * {@code primaryModeDisabled} is set to {@code true}, {@code isPrimaryMode()} is false,
    * the near cache is still functional via a cluster-wide listener, and a subsequent
    * codec-change notification does NOT re-enter primary mode.
    */
   public void testCleanupPrimaryModePermanentlyDisablesPrimaryMode() throws Exception {
      // Start in primary mode
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      RemoteCache<Integer, String> cache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      // Sanity: primaryModeDisabled is false before cleanup
      assertThat((Boolean) TestingUtil.extractField(inrc, "primaryModeDisabled"))
            .as("primaryModeDisabled must be false initially")
            .isFalse();

      // Invoke cleanupPrimaryMode() via reflection (simulates an irrecoverable registration failure)
      Method cleanupMethod = InvalidatedNearRemoteCache.class.getDeclaredMethod("cleanupPrimaryMode");
      cleanupMethod.setAccessible(true);
      cleanupMethod.invoke(inrc);

      // Primary mode must be permanently disabled and primary listeners torn down
      assertThat((Boolean) TestingUtil.extractField(inrc, "primaryModeDisabled"))
            .as("primaryModeDisabled must be true after cleanupPrimaryMode()")
            .isTrue();
      assertThat(inrc.isPrimaryMode())
            .as("isPrimaryMode() must return false after cleanupPrimaryMode()")
            .isFalse();
      assertThat(inrc.getServerListenerStates())
            .as("serverListenerStates must be cleared after cleanupPrimaryMode()")
            .isEmpty();

      // A codec-change notification must NOT re-enter primary mode (primaryModeDisabled = true)
      inrc.getDispatcher().notifyCodecChanged();
      Thread.sleep(200);

      assertThat(inrc.isPrimaryMode())
            .as("isPrimaryMode() must remain false after codec change when permanently disabled")
            .isFalse();
   }
}
