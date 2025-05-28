package org.infinispan.server.test.core.rollingupgrade;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;

public class RollingUpgradeConfigurationBuilder {
   private final String fromVersion;
   private final String toVersion;

   private int nodeCount = 3;
   private boolean xSite;
   private String jgroupsProtocol = "tcp";
   private int serverCheckTimeSecs = 30;
   private boolean useSharedDataMount = true;
   private BiConsumer<Throwable, RollingUpgradeHandler> exceptionHandler = (t, uh) -> { };

   private Consumer<RollingUpgradeHandler> initialHandler = uh -> {
      RemoteCacheManager rcm = uh.getRemoteCacheManager();
      RemoteCache<String, String> cache = rcm.administration().createCache("rolling-upgrade",
            new StringConfiguration("<replicated-cache></replicated-cache>"));
      cache.put("foo", "bar");
   };

   private Predicate<RollingUpgradeHandler> isValidServerState = uh -> {
      RemoteCacheManager rcm = uh.getRemoteCacheManager();
      RemoteCache<String, String> cache = rcm.getCache("rolling-upgrade");

      String value = cache.get("foo");
      if (value != null && !"bar".equals(value)) {
         throw new IllegalStateException("Remote cache returned " + value + " instead of bar");
      }

      Set<SocketAddress> servers = cache.getCacheTopologyInfo().getSegmentsPerServer().keySet();
      return servers.size() == uh.getConfiguration().nodeCount() +
            (uh.getCurrentState() == RollingUpgradeHandler.STATE.REMOVED_OLD ? -1 : 0);
   };

   public RollingUpgradeConfigurationBuilder(String fromVersion, String toVersion) {
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
   }

   public RollingUpgradeConfigurationBuilder nodeCount(int nodeCount) {
      if (nodeCount <= 0) {
         throw new IllegalArgumentException("nodeCount must be greater than 0");
      }
      this.nodeCount = nodeCount;
      return this;
   }

   public RollingUpgradeConfigurationBuilder xSite(boolean xSite) {
      this.xSite = xSite;
      return this;
   }

   public RollingUpgradeConfigurationBuilder jgroupsProtocol(String jgroupsProtocol) {
      this.jgroupsProtocol = Objects.requireNonNull(jgroupsProtocol);
      return this;
   }

   public RollingUpgradeConfigurationBuilder serverCheckTimeSecs(int serverCheckTimeSecs) {
      if (nodeCount <= 0) {
         throw new IllegalArgumentException("serverCheckTimeSecs must be greater than 0");
      }
      this.serverCheckTimeSecs = serverCheckTimeSecs;
      return this;
   }

   public RollingUpgradeConfigurationBuilder exceptionHandler(BiConsumer<Throwable, RollingUpgradeHandler> exceptionHandler) {
      this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
      return this;
   }

   public RollingUpgradeConfigurationBuilder sharedDataMount(boolean useSharedDataMount) {
      this.useSharedDataMount = useSharedDataMount;
      return this;
   }

   public RollingUpgradeConfigurationBuilder handlers(Consumer<RollingUpgradeHandler> initialHandler,
                                                      Predicate<RollingUpgradeHandler> isValidServerState) {
      this.initialHandler = Objects.requireNonNull(initialHandler);
      this.isValidServerState = Objects.requireNonNull(isValidServerState);
      return this;
   }

   public RollingUpgradeConfiguration build() {
      return new RollingUpgradeConfiguration(nodeCount, fromVersion, toVersion, xSite, jgroupsProtocol, serverCheckTimeSecs,
            useSharedDataMount, exceptionHandler, initialHandler, isValidServerState);
   }
}
