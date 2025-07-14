package org.infinispan.server.test.junit5;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.rollingupgrade.CombinedInfinispanServerDriver;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeXSiteHandler;
import org.junit.jupiter.api.extension.ExtensionContext;

public class RollingUpgradeHandlerXSiteExtension extends InfinispanXSiteServerExtension {
   private final Map<String, RollingUpgradeConfiguration> sites;
   private RollingUpgradeXSiteHandler handler;

   private RollingUpgradeHandlerXSiteExtension(Class<?> caller, Map<String, InfinispanServerExtensionBuilder> sites,
                                              String fromVersion, String toVersion) {
      super(new ArrayList<>());
      this.sites = sites.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e ->
         RollingUpgradeHandlerExtension.convertBuilder(caller.getName() + "-" + e.getKey(), e.getValue(), fromVersion, toVersion).build()));
   }

   public static RollingUpgradeHandlerXSiteExtension from(Class<?> caller, InfinispanXSiteServerExtensionBuilder builder,
                                                          String fromVersion, String toVersion) {
      return new RollingUpgradeHandlerXSiteExtension(caller, builder.siteConfigurations(), fromVersion, toVersion);
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
