package org.infinispan.xsite.status;

import java.util.Map;

import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.XSiteResponse;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface TakeOfflineManager {

   void registerRequest(XSiteResponse response);

   boolean isOffline(String siteName);

   boolean containsSite(String siteName);

   boolean notContainsSite(String siteName);

   void amendConfiguration(String siteName, Integer afterFailures, Long minTimeToWait);

   TakeOfflineConfiguration getConfiguration(String siteName);

   /**
    * Returns a Map having as entries the site names and as value Boolean.TRUE if the site is online and Boolean.FALSE
    * if it is offline.
    */
   Map<String, Boolean> status();

   /**
    * Brings a site with the given name back online.
    */
   BringSiteOnlineResponse bringSiteOnline(String siteName);

   TakeSiteOfflineResponse takeSiteOffline(String siteName);

}
