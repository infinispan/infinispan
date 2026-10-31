package org.infinispan.client.hotrod.near;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
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
 * Tests the permanent-disable path ({@code cleanupPrimaryMode}) and the upgrade-failure path
 * ({@code switchToPrimaryListenerMode}) of primary-owner near cache mode.
 *
 * <p>{@link #testCleanupPrimaryModePermanentlyDisablesPrimaryMode} — verifies that invoking
 * {@code cleanupPrimaryMode()} sets {@code primaryModeDisabled} permanently and leaves the near cache
 * in cluster-wide mode. Subsequent codec-change notifications must not trigger a re-upgrade attempt.</p>
 *
 * <p>{@link #testSwitchToPrimaryModeFailureReinstallsClusterListener} — verifies that invoking
 * {@code switchToPrimaryListenerMode()} when primary listeners cannot be established (because no
 * primary segment information is available) falls back to re-installing the cluster-wide listener
 * without permanently disabling primary mode.</p>
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
    * codec-change notification does NOT trigger a re-upgrade.
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

      // A codec-change notification must NOT trigger a re-upgrade (primaryModeDisabled = true)
      inrc.getDispatcher().notifyCodecChanged();
      Thread.sleep(200);

      assertThat(inrc.isPrimaryMode())
            .as("isPrimaryMode() must remain false after codec change when permanently disabled")
            .isFalse();
   }

   /**
    * Verifies {@code switchToPrimaryListenerMode()} failure-and-fallback semantics: when primary
    * listener registration fails during the re-upgrade, the method must not permanently disable
    * primary mode and must re-install the cluster-wide listener.
    *
    * <p>The client uses {@code HASH_DISTRIBUTION_AWARE} so that {@code canUsePrimaryMode()} is true and
    * the registration (and its failure handling) is actually executed.</p>
    */
   public void testSwitchToPrimaryModeFailureReinstallsClusterListener() throws Exception {
      RemoteCacheManager clientManager = createNearClient(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      RemoteCache<Integer, String> cache = clientManager.getCache();
      InvalidatedNearRemoteCache<Integer, String> inrc = (InvalidatedNearRemoteCache<Integer, String>) cache;
      assertThat(inrc.isPrimaryMode()).isTrue();

      OperationDispatcher dispatcher = inrc.getDispatcher();
      SocketAddress server0Addr = InetSocketAddress.createUnresolved(server(0).getHost(), server(0).getPort());
      OperationChannel targetChannel = dispatcher.getHandlerForAddress(server0Addr);
      assertThat(targetChannel).isNotNull();
      Codec originalCodec = TestingUtil.extractField(targetChannel, "codec");
      Codec codec41 = ProtocolVersion.PROTOCOL_VERSION_41.getCodec();

      CacheOperationsFactory originalCof = TestingUtil.extractField(inrc, "cacheOperationsFactory");
      AtomicInteger primaryOps = new AtomicInteger();
      AtomicBoolean failed = new AtomicBoolean();
      // The first primary operation creates the event dispatcher; the ones after that are the per-server registrations
      CacheOperationsFactory failingCof = (CacheOperationsFactory) Proxy.newProxyInstance(
            CacheOperationsFactory.class.getClassLoader(),
            new Class<?>[]{CacheOperationsFactory.class},
            (proxy, method, args) -> {
               if ("newAddPrimaryNearCacheListenerOperation".equals(method.getName()) && primaryOps.incrementAndGet() > 1) {
                  failed.set(true);
                  throw new IllegalStateException("Forced primary listener registration failure");
               }
               try {
                  return method.invoke(originalCof, args);
               } catch (InvocationTargetException e) {
                  throw e.getCause();
               }
            });

      try {
         // Downgrade to cluster mode
         TestingUtil.replaceField(codec41, "codec", targetChannel, OperationChannel.class);
         dispatcher.notifyCodecChanged();
         eventually(() -> !inrc.isPrimaryMode() && inrc.getNearCacheService().isConnected());

         // Re-upgrade, but fail the primary registration
         TestingUtil.replaceField(failingCof, "cacheOperationsFactory", inrc, InvalidatedNearRemoteCache.class);
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
         dispatcher.notifyCodecChanged();
         eventually(failed::get);
         // Wait for the (failed) transition to complete, it must have re-installed the cluster listener
         AtomicBoolean modeSwitch = TestingUtil.extractField(inrc, "modeSwitch");
         eventually(() -> !modeSwitch.get() && inrc.getNearCacheService().isConnected());
      } finally {
         TestingUtil.replaceField(originalCof, "cacheOperationsFactory", inrc, InvalidatedNearRemoteCache.class);
         TestingUtil.replaceField(originalCodec, "codec", targetChannel, OperationChannel.class);
      }

      assertThat(inrc.isPrimaryMode())
            .as("Primary mode must not be entered when primary registration fails")
            .isFalse();
      assertThat((Boolean) TestingUtil.extractField(inrc, "primaryModeDisabled"))
            .as("A failed upgrade must not permanently disable primary mode")
            .isFalse();

      // Basic operations must work
      cache.put(2, "world");
      assertThat(cache.get(2)).isEqualTo("world");
      eventually(() -> inrc.getNearCacheService().get(2) != null);
   }
}
