package org.infinispan.server.test.core.rollingupgrade;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;

public class UpgradeConfigurationBuilder {
   private final String fromVersion;
   private final String toVersion;

   private int nodeCount = 3;
   private boolean xSite;
   private String jgroupsProtocol = "tcp";
   private int serverCheckTimeSecs = 30;
   private boolean useSharedDataMount = true;
   private BiConsumer<Throwable, UpgradeHandler> exceptionHandler = (t, uh) -> {
      t.printStackTrace();
   };

   private Consumer<String> logConsumer = System.out::println;

   private Function<RemoteCacheManager, RemoteCache<String, String>> initialHandler = rcm -> {
      RemoteCache<String, String> cache = rcm.administration().createCache("rolling-upgrade",
            new StringConfiguration("<replicated-cache></replicated-cache>"));
      cache.put("foo", "bar");
      return cache;
   };

   public UpgradeConfigurationBuilder(String fromVersion, String toVersion) {
      this.fromVersion = fromVersion;
      this.toVersion = toVersion;
   }

   public UpgradeConfigurationBuilder nodeCount(int nodeCount) {
      if (nodeCount <= 0) {
         throw new IllegalArgumentException("nodeCount must be greater than 0");
      }
      this.nodeCount = nodeCount;
      return this;
   }

   public UpgradeConfigurationBuilder xSite(boolean xSite) {
      this.xSite = xSite;
      return this;
   }

   public UpgradeConfigurationBuilder jgroupsProtocol(String jgroupsProtocol) {
      this.jgroupsProtocol = Objects.requireNonNull(jgroupsProtocol);
      return this;
   }

   public UpgradeConfigurationBuilder serverCheckTimeSecs(int serverCheckTimeSecs) {
      if (nodeCount <= 0) {
         throw new IllegalArgumentException("serverCheckTimeSecs must be greater than 0");
      }
      this.serverCheckTimeSecs = serverCheckTimeSecs;
      return this;
   }

   public UpgradeConfigurationBuilder exceptionHandler(BiConsumer<Throwable, UpgradeHandler> exceptionHandler) {
      this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
      return this;
   }

   public UpgradeConfigurationBuilder sharedDataMount(boolean useSharedDataMount) {
      this.useSharedDataMount = useSharedDataMount;
      return this;
   }

   public UpgradeConfigurationBuilder logConsumer(Consumer<String> logConsumer) {
      this.logConsumer = Objects.requireNonNull(logConsumer);
      return this;
   }

   public UpgradeConfigurationBuilder initialHandler(Function<RemoteCacheManager, RemoteCache<String, String>> initialHandler) {
      this.initialHandler = Objects.requireNonNull(initialHandler);
      return this;
   }

   public UpgradeConfiguration build() {
      return new UpgradeConfiguration(nodeCount, fromVersion, toVersion, xSite, jgroupsProtocol, serverCheckTimeSecs,
            useSharedDataMount, exceptionHandler, logConsumer, initialHandler);
   }
}
