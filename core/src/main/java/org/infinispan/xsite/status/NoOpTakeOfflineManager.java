package org.infinispan.xsite.status;

import java.util.Collections;
import java.util.Map;

import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.remoting.transport.XSiteResponse;

/**
 * An empty {@link TakeOfflineManager} implementation for caches which don't backup any data to remote sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class NoOpTakeOfflineManager implements TakeOfflineManager {

   private static final NoOpTakeOfflineManager INSTANCE = new NoOpTakeOfflineManager();

   private NoOpTakeOfflineManager() {
   }

   public static NoOpTakeOfflineManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void registerRequest(XSiteResponse response) {
      //no-op
   }

   @Override
   public SiteState getSiteState(String siteName) {
      return SiteState.NOT_FOUND;
   }

   @Override
   public void amendConfiguration(String siteName, Integer afterFailures, Long minTimeToWait) {
      //no-op
   }

   @Override
   public TakeOfflineConfiguration getConfiguration(String siteName) {
      //non-existing
      return null;
   }

   @Override
   public Map<String, Boolean> status() {
      return Collections.emptyMap();
   }

   @Override
   public BringSiteOnlineResponse bringSiteOnline(String siteName) {
      return BringSiteOnlineResponse.BSOR_NO_SUCH_SITE;
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      return TakeSiteOfflineResponse.TSOR_NO_SUCH_SITE;
   }
}
