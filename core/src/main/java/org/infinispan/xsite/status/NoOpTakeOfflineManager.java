package org.infinispan.xsite.status;

import java.util.Collections;
import java.util.Map;

import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.remoting.transport.XSiteResponse;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class NoOpTakeOfflineManager implements TakeOfflineManager {

   private static final NoOpTakeOfflineManager INSTANCE = new NoOpTakeOfflineManager();
   private NoOpTakeOfflineManager() {}

   public static NoOpTakeOfflineManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void registerRequest(XSiteResponse response) {
      //no-op
   }

   @Override
   public boolean isOffline(String siteName) {
      return false;
   }

   @Override
   public boolean containsSite(String siteName) {
      return false;
   }

   @Override
   public boolean notContainsSite(String siteName) {
      return true;
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
      return BringSiteOnlineResponse.NO_SUCH_SITE;
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      return TakeSiteOfflineResponse.NO_SUCH_SITE;
   }
}
