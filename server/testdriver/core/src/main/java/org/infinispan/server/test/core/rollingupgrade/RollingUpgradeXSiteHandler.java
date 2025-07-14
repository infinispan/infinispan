package org.infinispan.server.test.core.rollingupgrade;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class RollingUpgradeXSiteHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final Map<String, RollingUpgradeHandler> sites;

   private RollingUpgradeXSiteHandler(Map<String, RollingUpgradeHandler> sites) {
      this.sites = sites;
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
      Throwable throwableEncountered = null;
      try {
         for (Map.Entry<String, RollingUpgradeHandler> entry : sites.entrySet()) {
            String siteName = entry.getKey();
            RollingUpgradeHandler ruh = entry.getValue();
            try {
               ruh.completeUpgrade(false);
            } catch (Throwable t) {
               log.errorf(t, "Exception encountered with site %s", siteName);
               ruh.exceptionEncountered(t);
               // Propagate the first exception as it may cause the later ones
               if (throwableEncountered == null) {
                  throwableEncountered = t;
               }
            }
         }
         if (throwableEncountered != null) {
            throw Util.unchecked(throwableEncountered);
         }
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
