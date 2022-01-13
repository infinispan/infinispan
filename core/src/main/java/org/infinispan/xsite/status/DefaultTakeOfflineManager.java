package org.infinispan.xsite.status;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.CacheUnreachableException;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
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
 * It automatically takes a site offline when certain failures condition happens.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultTakeOfflineManager implements TakeOfflineManager, XSiteResponse.XSiteResponseCompleted {

   private static final Log log = LogFactory.getLog(DefaultTakeOfflineManager.class);

   private final String cacheName;
   private final Map<String, OfflineStatus> offlineStatus;

   @Inject TimeService timeService;
   @Inject Configuration config;
   @Inject EventLogManager eventLogManager;
   @Inject RpcManager rpcManager;
   @Inject InternalDataContainer<Object, Object> dataContainer;

   public DefaultTakeOfflineManager(String cacheName) {
      this.cacheName = cacheName;
      offlineStatus = new ConcurrentHashMap<>(8);
   }

   public static boolean isCommunicationError(Throwable throwable) {
      Throwable error = throwable;
      if (throwable instanceof ExecutionException) {
         error = throwable.getCause();
      }
      return error instanceof TimeoutException ||
            error instanceof org.infinispan.util.concurrent.TimeoutException ||
            error instanceof UnreachableException ||
            error instanceof CacheUnreachableException ||
            error instanceof SuspectException;
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

   @Start
   public void start() {
      String localSiteName = rpcManager.getTransport().localSiteName();
      config.sites().enabledBackupStream()
            .filter(bc -> !localSiteName.equals(bc.site()))
            .forEach(bc -> {
               String siteName = bc.site();
               OfflineStatus offline = new OfflineStatus(bc.takeOffline(), timeService, new Listener(siteName));
               offlineStatus.put(siteName, offline);
            });
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
      OfflineStatus status = offlineStatus.get(siteName);
      if (status == null) {
         log.tryingToBringOnlineNonexistentSite(siteName);
         return BringSiteOnlineResponse.NO_SUCH_SITE;
      } else {
         return status.bringOnline() ? BringSiteOnlineResponse.BROUGHT_ONLINE : BringSiteOnlineResponse.ALREADY_ONLINE;
      }
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      OfflineStatus status = offlineStatus.get(siteName);
      if (status == null) {
         return TakeSiteOfflineResponse.NO_SUCH_SITE;
      } else {
         return status.forceOffline() ? TakeSiteOfflineResponse.TAKEN_OFFLINE
               : TakeSiteOfflineResponse.ALREADY_OFFLINE;
      }
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

   @Override
   public String toString() {
      return "DefaultTakeOfflineManager{cacheName='" + cacheName + "'}";
   }

   private EventLogger getEventLogger() {
      return eventLogManager.getEventLogger().context(cacheName).scope(rpcManager.getAddress());
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
