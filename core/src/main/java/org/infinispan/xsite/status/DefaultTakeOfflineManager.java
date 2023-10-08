package org.infinispan.xsite.status;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CrossSiteIllegalLifecycleStateException;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metrics.Constants;
import org.infinispan.metrics.impl.MetricUtils;
import org.infinispan.remoting.CacheUnreachableException;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.notification.SiteStatusListener;
import org.jgroups.UnreachableException;

/**
 * The default implementation of {@link TakeOfflineManager}.
 * <p>
 * It automatically takes a site offline when failures happen.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultTakeOfflineManager implements TakeOfflineManager, XSiteResponse.XSiteResponseCompleted {

   private static final Log log = LogFactory.getLog(DefaultTakeOfflineManager.class);
   private static final String SCOPE = "site_status";

   private final Map<String, OfflineStatus> offlineStatus;

   private TimeService timeService;
   private EventLogger eventLogger;
   @Inject InternalDataContainer<Object, Object> dataContainer;

   public DefaultTakeOfflineManager() {
      offlineStatus = new ConcurrentHashMap<>(8);
   }

   public static boolean isCommunicationError(Throwable throwable) {
      Throwable error = throwable;
      if (throwable instanceof ExecutionException) {
         error = throwable.getCause();
      }
      return error instanceof TimeoutException ||
            error instanceof org.infinispan.commons.TimeoutException ||
            error instanceof UnreachableException ||
            error instanceof CacheUnreachableException ||
            error instanceof SuspectException ||
            error instanceof CrossSiteIllegalLifecycleStateException;
   }

   private static Optional<CacheConfigurationException> findConfigurationError(Throwable throwable) {
      Throwable error = throwable;
      if (throwable instanceof ExecutionException || throwable instanceof RemoteException) {
         error = throwable.getCause();
      }
      return error instanceof CacheConfigurationException ? //incorrect cache configuration in remote site?
            Optional.of((CacheConfigurationException) error) :
            Optional.empty();
   }

   @Inject
   public void inject(EmbeddedCacheManager cacheManager, Configuration configuration, Transport transport, TimeService timeService, EventLogManager eventLogManager, @ComponentName(KnownComponentNames.CACHE_NAME) String cacheName) {
      this.timeService = timeService;
      this.eventLogger = eventLogManager.getEventLogger().context(cacheName).scope(transport.getAddress());
      if (!offlineStatus.isEmpty()) {
         // method can be invoked multiple times
         return;
      }
      Supplier<TimeService> timeServiceSupplier = this::getTimeService;
      var localSiteName = transport.localSiteName();
      Cache<ScopedState, Long> globalSiteStatus = cacheManager.getCache(InternalCacheNames.CONFIG_STATE_CACHE_NAME);
      configuration.sites().allBackupsStream()
            .filter(bc -> !localSiteName.equals(bc.site()))
            .forEach(bc -> {
               var siteName = bc.site();
               var offline = new OfflineStatus(bc.takeOffline(), timeServiceSupplier, new Listener(siteName),
                     new ScopedState(SCOPE, cacheName + ":" + siteName), globalSiteStatus);
               offlineStatus.put(siteName, offline);
            });
   }

   @Start
   public void start() {
      offlineStatus.values().forEach(OfflineStatus::start);
   }

   @Stop
   public void stop() {
      offlineStatus.values().forEach(OfflineStatus::stop);
      offlineStatus.clear();
   }

   @Override
   public void registerRequest(XSiteResponse<?> response) {
      response.whenCompleted(this);
   }

   @Override
   public SiteState getSiteState(String siteName) {
      OfflineStatus offline = offlineStatus.get(siteName);
      if (offline == null) {
         return SiteState.NOT_FOUND;
      }
      return offline.isOffline() ? SiteState.OFFLINE : SiteState.ONLINE;
   }

   @Override
   public void amendConfiguration(String siteName, Integer afterFailures, Long minTimeToWait) {
      OfflineStatus status = offlineStatus.get(siteName);
      if (status == null) {
         return;
      }
      status.amend(afterFailures, minTimeToWait);
   }

   @Override
   public TakeOfflineConfiguration getConfiguration(String siteName) {
      OfflineStatus status = offlineStatus.get(siteName);
      return status == null ? null : status.getTakeOffline();
   }

   @Override
   public Map<String, Boolean> status() {
      Map<String, Boolean> result = new HashMap<>(offlineStatus.size());
      for (Map.Entry<String, OfflineStatus> os : offlineStatus.entrySet()) {
         result.put(os.getKey(), !os.getValue().isOffline());
      }
      return result;
   }

   @Override
   public BringSiteOnlineResponse bringSiteOnline(String siteName) {
      var status = offlineStatus.get(siteName);
      if (status == null) {
         log.tryingToBringOnlineNonexistentSite(siteName);
         return BringSiteOnlineResponse.BSOR_NO_SUCH_SITE;
      } else {
         return CompletionStages.join(status.bringOnline()) ? BringSiteOnlineResponse.BSOR_BROUGHT_ONLINE : BringSiteOnlineResponse.BSOR_ALREADY_ONLINE;
      }
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      var status = offlineStatus.get(siteName);
      if (status == null) {
         return TakeSiteOfflineResponse.TSOR_NO_SUCH_SITE;
      } else {
         return CompletionStages.join(status.forceOffline()) ? TakeSiteOfflineResponse.TSOR_TAKEN_OFFLINE
               : TakeSiteOfflineResponse.TSOR_ALREADY_OFFLINE;
      }
   }

   @Override
   public Collection<MetricInfo> getCustomMetrics(GlobalMetricsConfiguration configuration) {
      List<MetricInfo> attributes = new ArrayList<>(offlineStatus.size() * 3);
      for (Map.Entry<String, OfflineStatus> entry : offlineStatus.entrySet()) {
         OfflineStatus status = entry.getValue();
         Map<String, String> tags = Map.of(Constants.SITE_TAG_NAME, entry.getKey());

         if (configuration.namesAsTags()) {
            attributes.add(MetricUtils.createGauge("status", entry.getKey() + " status. 1=online, 0=offline", o -> status.isOffline() ? 0 : 1, tags));
            attributes.add(MetricUtils.createGauge("failures_count", "Number of consecutive failures to " + entry.getKey(), o -> status.getFailureCount(), tags));
            attributes.add(MetricUtils.createGauge("millis_since_first_failure", "Milliseconds from first consecutive failure to " + entry.getKey(), o -> status.millisSinceFirstFailure(), tags));
         } else {
            String lowerCaseSite = entry.getKey().toLowerCase();
            attributes.add(MetricUtils.createGauge(lowerCaseSite + "_status", entry.getKey() + " status. 1=online, 0=offline", o -> status.isOffline() ? 0 : 1, tags));
            attributes.add(MetricUtils.createGauge(lowerCaseSite + "_failures_count", "Number of consecutive failures to " + entry.getKey(), o -> status.getFailureCount(), tags));
            attributes.add(MetricUtils.createGauge(lowerCaseSite + "_millis_since_first_failure", "Milliseconds from first consecutive failure to " + entry.getKey(), o -> status.millisSinceFirstFailure(), tags));
         }


      }
      return attributes;
   }

   @Override
   public void onCompleted(XSiteBackup backup, long sendTimeNanos, long durationNanos, Throwable throwable) {
      OfflineStatus status = offlineStatus.get(backup.getSiteName());
      assert status != null;

      Optional<CacheConfigurationException> e = findConfigurationError(throwable);
      if (e.isPresent()) {
         //we have an invalid configuration. change site to offline
         log.xsiteInvalidConfigurationRemoteSite(backup.getSiteName(), e.get());
         status.forceOffline();
         return;
      }

      if (status.isEnabled()) {
         if (isCommunicationError(throwable)) {
            status.updateOnCommunicationFailure(TimeUnit.NANOSECONDS.toMillis(sendTimeNanos));
         } else if (!status.isOffline()) {
            status.reset();
         }
      }
   }

   public OfflineStatus getOfflineStatus(String siteName) {
      return offlineStatus.get(siteName);
   }

   private EventLogger getEventLogger() {
      return eventLogger;
   }

   public TimeService getTimeService() {
      return timeService;
   }

   private final class Listener implements SiteStatusListener {

      private final String siteName;

      Listener(String siteName) {
         this.siteName = siteName;
      }

      @Override
      public void siteOnline() {
         getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOnline(siteName));
      }

      @Override
      public void siteOffline() {
         getEventLogger().info(EventLogCategory.CLUSTER, MESSAGES.siteOffline(siteName));
         log.debug("Touching all in memory entries as a site has gone offline");
         long currentTimeMillis = timeService.wallClockTime();
         dataContainer.forEachSegment((map, segment) -> map.touchAll(currentTimeMillis));
      }

      @Override
      public String toString() {
         return "Listener{" +
               "siteName='" + siteName + '\'' +
               '}';
      }
   }
}
