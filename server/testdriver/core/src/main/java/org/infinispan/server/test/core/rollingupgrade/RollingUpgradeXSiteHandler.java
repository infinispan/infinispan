package org.infinispan.server.test.core.rollingupgrade;

import java.util.HashMap;
import java.util.Map;

public class RollingUpgradeXSiteHandler {
   private final Map<String, RollingUpgradeHandler> sites;

   private RollingUpgradeXSiteHandler(Map<String, RollingUpgradeHandler> sites) {
      this.sites = sites;
   }

   public static void performUpgrade(String siteName1, RollingUpgradeConfiguration configuration1,
                                     String siteName2, RollingUpgradeConfiguration configuration2) {
      RollingUpgradeHandler ruh1 = RollingUpgradeHandler.startOldCluster(configuration1, siteName1);
      RollingUpgradeHandler ruh2 = RollingUpgradeHandler.startOldCluster(configuration2, siteName2);

      // TODO: ascertain site is working??
      try {
         ruh1.completeUpgrade(false);
         ruh2.completeUpgrade(false);
      } finally {
         ruh1.close();
         ruh2.close();
      }
   }

   public static RollingUpgradeXSiteHandler startOldClusters(Map<String, RollingUpgradeConfiguration> sites) {
      Map<String, RollingUpgradeHandler> siteHandlers = new HashMap<>(sites.size());
      for (Map.Entry<String, RollingUpgradeConfiguration> site : sites.entrySet()) {
         siteHandlers.put(site.getKey(), RollingUpgradeHandler.startOldCluster(site.getValue(), site.getKey()));
      }

      return new RollingUpgradeXSiteHandler(siteHandlers);
   }

   public RollingUpgradeHandler handlerForSite(String siteName) {
      return sites.get(siteName);
   }

   public void completeUpgrade(boolean shouldClose) {
      try {
         sites.forEach((siteName, ruh) -> ruh.completeUpgrade(false));
      } finally {
         if (shouldClose) {
            close();
         }
      }
   }

   public void close() {
      sites.forEach((siteName, ruh) -> ruh.close());
   }

   public void exceptionEncountered(Throwable throwable) {
      sites.forEach((siteName, ruh) -> ruh.exceptionEncountered(throwable));
   }
}
