package org.infinispan.xsite.status;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.TakeOfflineConfigurationBuilder;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.metrics.impl.CustomMetricsSupplier;
import org.infinispan.remoting.transport.XSiteResponse;

/**
 * It keeps tracks of cross-site requests to take sites offline when certain failures conditions happen.
 * <p>
 * Those condition are configured in {@link TakeOfflineConfiguration}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public interface TakeOfflineManager extends CustomMetricsSupplier {

   /**
    * Registers a cross-site request made.
    * <p>
    * Handles the response for the request and takes action in case of failure.
    *
    * @param response The cross-site response.
    */
   void registerRequest(XSiteResponse<?> response);

   /**
    * Returns the site state for site {@code siteName}.
    * <p>
    * The site can be {@link SiteState#ONLINE} or {@link SiteState#OFFLINE}. If it doesn't exist, {@link
    * SiteState#NOT_FOUND} is returned.
    *
    * @param siteName The remote site name.
    * @return The {@link SiteState}.
    */
   SiteState getSiteState(String siteName);

   /**
    * It changes the {@link TakeOfflineConfiguration} for site {@code siteName}.
    * <p>
    * If the {@code siteName} doesn't exist, this method is a no-op.
    *
    * @param siteName      The remote site name.
    * @param afterFailures The new {@link TakeOfflineConfigurationBuilder#afterFailures(int)} or {@code null} for no
    *                      changes.
    * @param minTimeToWait The new {@link TakeOfflineConfigurationBuilder#minTimeToWait(long)} or {@code null} for no
    *                      changes.
    */
   void amendConfiguration(String siteName, Integer afterFailures, Long minTimeToWait);

   /**
    * It returns the current {@link TakeOfflineConfiguration} for site {@code siteName}.
    *
    * @param siteName The remote site name.
    * @return The current {@link TakeOfflineConfiguration} or {@code null} if the site {@code siteName} doesn't exist.
    */
   TakeOfflineConfiguration getConfiguration(String siteName);

   /**
    * It returns a {@link Map} with the sites name and their state (Online or Offline).
    * <p>
    * If a site is online, then its value is {@link Boolean#TRUE}, otherwise is {@link Boolean#FALSE}.
    *
    * @return A {@link Map} with the site state.
    */
   Map<String, Boolean> status();

   /**
    * It changes the site {@code siteName} to online.
    * <p>
    * If the site is already online, then {@link BringSiteOnlineResponse#BSOR_ALREADY_ONLINE} is returned. If it doesn't
    * exits, {@link BringSiteOnlineResponse#BSOR_NO_SUCH_SITE} is returned.
    *
    * @param siteName The remote site name.
    * @return The {@link BringSiteOnlineResponse}.
    */
   BringSiteOnlineResponse bringSiteOnline(String siteName);

   /**
    * It changes the site {@code siteName} to offline.
    * <p>
    * If the site is already offline, then {@link TakeSiteOfflineResponse#TSOR_ALREADY_OFFLINE} is returned. If it doesn't
    * exits, {@link TakeSiteOfflineResponse#TSOR_NO_SUCH_SITE} is returned.
    *
    * @param siteName The remote site name.
    * @return The {@link TakeSiteOfflineResponse}.
    */
   TakeSiteOfflineResponse takeSiteOffline(String siteName);

   @Override
   default Collection<MetricInfo> getCustomMetrics(GlobalMetricsConfiguration configuration) {
      return Collections.emptyList();
   }
}
