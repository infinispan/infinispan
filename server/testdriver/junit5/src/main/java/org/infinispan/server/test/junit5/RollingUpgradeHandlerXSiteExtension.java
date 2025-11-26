package org.infinispan.server.test.junit5;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.rollingupgrade.CombinedInfinispanServerDriver;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeVersion;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeXSiteHandler;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RollingUpgradeHandlerXSiteExtension extends InfinispanXSiteServerExtension {
   private final Map<String, RollingUpgradeConfiguration> sites;
   private RollingUpgradeXSiteHandler handler;

   private RollingUpgradeHandlerXSiteExtension(Class<?> caller, Map<String, InfinispanServerExtensionBuilder> sites,
                                               RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion,
                                               Consumer<RollingUpgradeConfigurationBuilder> decorator) {
      super(new ArrayList<>());
      this.sites = sites.entrySet().stream()
            .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  e -> {
                     RollingUpgradeConfigurationBuilder builder = RollingUpgradeHandlerExtension.convertBuilder(caller.getName() + "-" + e.getKey(), e.getValue(), fromVersion, toVersion);
                     decorator.accept(builder);
                     return builder.build();
                  }));
   }

   public static RollingUpgradeHandlerXSiteExtension from(Class<?> caller, InfinispanXSiteServerExtensionBuilder builder,
                                                          RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion) {
      return from(caller, builder, fromVersion, toVersion, ignore -> {});
   }

   public static RollingUpgradeHandlerXSiteExtension from(Class<?> caller, InfinispanXSiteServerExtensionBuilder builder,
                                                          RollingUpgradeVersion fromVersion, RollingUpgradeVersion toVersion,
                                                          Consumer<RollingUpgradeConfigurationBuilder> decorator) {
      return new RollingUpgradeHandlerXSiteExtension(caller, builder.siteConfigurations(), fromVersion, toVersion, decorator);
   }

   @Override
   protected void onTestsStart(ExtensionContext extensionContext) throws InterruptedException {
      if (handler == null) {
         handler = RollingUpgradeXSiteHandler.startOldClusters(sites);
         for (String siteName : sites.keySet()) {
            RollingUpgradeHandler handler = this.handler.handlerForSite(siteName);
            // We take a more extreme approach that every site has a mixed cluster with one node being upgraded
            handler.upgradeNewNode();
            testServers.add(new TestServer(handler.getFromConfig(), new CombinedInfinispanServerDriver(
                  handler.getFromDriver(), handler.getToDriver())));
         }
      }
   }

   @Override
   protected void onTestsComplete(ExtensionContext extensionContext) {
      if (handler != null) {
         testServers.forEach(TestServer::afterListeners);
         try {
            if (extensionContext.getExecutionException().isPresent()) {
               handler.exceptionEncountered(extensionContext.getExecutionException().get());
            } else {
               handler.completeUpgrade(false);
            }
         } finally {
            handler.close();
         }
      }
   }
}
