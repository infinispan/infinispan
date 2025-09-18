package org.infinispan.xsite.events;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.SiteViewChanged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.SitesViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteCacheMapper;
import org.infinispan.xsite.XSiteNamedCache;
import org.infinispan.xsite.commands.XSiteLocalEventCommand;
import org.infinispan.xsite.commands.remote.XSiteRemoteEventCommand;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Default implementation of {@link XSiteEventsManager}.
 *
 * @since 15.0
 */
@Scope(Scopes.GLOBAL)
@Listener
public class XSiteEventsManagerImpl implements XSiteEventsManager {

   private static final int[] BACK_OFF_DELAYS = {200, 500, 1000, 2000, 5000};
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject Transport transport;
   @Inject CacheManagerNotifier notifier;
   @Inject GlobalComponentRegistry globalRegistry;
   @Inject XSiteCacheMapper xSiteCacheMapper;
   @Inject MetricsRegistry metricsRegistry;
   private Executor backOffExecutor;

   private final XSiteViewMetrics xSiteViewMetrics;

   public XSiteEventsManagerImpl() {
      xSiteViewMetrics = new XSiteViewMetrics(this::getMetricsRegistry, this::getTransport);
   }

   @Start
   public void start() {
      notifier.addListener(this);
   }

   @Stop
   public void stop() {
      notifier.removeListener(this);
      xSiteViewMetrics.stop();
   }

   @Inject
   public void createExecutor(BlockingManager blockingManager) {
      backOffExecutor = blockingManager.asExecutor("x-site-evt-backoff");
   }

   @Override
   public CompletionStage<Void> onLocalEvents(List<XSiteEvent> events) {
      log.debugf("Local events received: %s", events);
      try (var holder = new XSiteEventSender(this::sendWithBackOff)) {
         for (var e : events) {
            switch (e.getType()) {
               case SITE_CONNECTED:
                  onRemoteSiteConnected(e.getSiteName(), holder);
                  break;
               case STATE_REQUEST:
               case INITIAL_STATE_REQUEST:
                  onRemoteSiteStateRequest(e.getSiteName(), e.getCacheName(), e.getType() == XSiteEventType.INITIAL_STATE_REQUEST);
                  break;
               default:
                  log.debugf("Unknown event received: %s", e);
            }
         }
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> onRemoteEvents(List<XSiteEvent> events) {
      log.debugf("Remote events received: %s", events);
      if (transport.isCoordinator()) {
         return onLocalEvents(events);
      }
      try {
         log.debugf("Forwarding events to coordinator: %s", events);
         transport.sendTo(transport.getCoordinator(), new XSiteLocalEventCommand(events), DeliverOrder.PER_SENDER_NO_FC);
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
      return CompletableFutures.completedNull();
   }

   @SiteViewChanged
   public void onSiteViewChanged(SitesViewChangedEvent event) {
      if (!transport.isPrimaryRelayNode()) {
         return;
      }
      // This event is only triggered in SiteMaster nodes.
      xSiteViewMetrics.onNewCrossSiteView(event.getJoiners(), event.getLeavers());
      log.debugf("On site view changed event: %s", event);
      event.getJoiners().stream()
            .filter(this::isRemoteSite)
            .forEach(this::sendNewConnectionEvent);
   }

   @CacheStarted
   public void onCacheStarted(CacheStartedEvent event) {
      log.debugf("On cache started (is coordinator? %s): %s", transport.isCoordinator(), event.getCacheName());
      // If any site is configured, add metrics for it.
      xSiteCacheMapper.sitesNameFromCache(event.getCacheName())
            .filter(this::isRemoteSite)
            .forEach(this::registerMetricForSite);
      if (!transport.isCoordinator()) {
         return;
      }
      try (var sender = new XSiteEventSender(this::sendWithBackOff)) {
         xSiteCacheMapper.findRemoteCachesWithAsyncBackup(event.getCacheName())
               .forEach(i -> sender.addEventToSite(i.siteName(), XSiteEvent.createInitialStateRequest(localSite(), i.cacheName())));
      } catch (Exception e) {
         log.debugf(e, "Unable to send state request for cache %s", event.getCacheName());
      }
   }

   @ViewChanged
   public void onViewChanged(ViewChangedEvent event) {
      if (transport.isPrimaryRelayNode()) {
         // The node is a SiteMaster, or it is being promoted to SiteMaster.
         // Update the metrics.
         xSiteViewMetrics.onSiteCoordinatorPromotion(transport.getSitesView());
      } else {
         // A node can lose its SiteMaster role, for example, during the merge event (in cluster split brain).
         // Mark all unknown to signal the results from this node is not reliable.
         xSiteViewMetrics.markAllUnknown();
      }
   }

   private void registerMetricForSite(String siteName) {
      // A non SiteMaster returns an empty list. Change to null to signal that the metric is unreliable.
      var siteView = transport.isPrimaryRelayNode() ? transport.getSitesView() : null;
      xSiteViewMetrics.onNewSiteFound(siteName, siteView);
   }

   private void onRemoteSiteConnected(ByteString site, XSiteEventSender sender) {
      for (var it = xSiteCacheMapper.remoteCachesFromSite(site).iterator(); it.hasNext(); ) {
         sender.addEventToSite(site, XSiteEvent.createRequestState(localSite(), it.next()));
      }
   }

   private void sendNewConnectionEvent(String remoteSite) {
      var cmd = new XSiteRemoteEventCommand(List.of(XSiteEvent.createConnectEvent(localSite())));
      var backup = new XSiteBackup(remoteSite, false, 10000);
      log.debugf("Sending connection event to %s: %s", backup, cmd);
      sendWithBackOff(backup, cmd);
   }

   private void onRemoteSiteStateRequest(ByteString remoteSite, ByteString localCacheName, boolean initialState) {
      var cacheRegistry = globalRegistry.getNamedComponentRegistry(localCacheName);
      if (cacheRegistry == null) {
         log.debugf("State Transfer request from site '%s' and cache '%s' failed. Cache does no exist.", remoteSite, localCacheName);
         return;
      }
      var xsiteStateManagerRef = cacheRegistry.getXSiteStateTransferManager();
      if (!xsiteStateManagerRef.isRunning()) {
         log.debugf("State Transfer request from site '%s' and cache '%s' failed. Cache is not started.", remoteSite, localCacheName);
         return;
      }
      xsiteStateManagerRef.running().startAutomaticStateTransferTo(remoteSite, initialState);
   }

   private ByteString localSite() {
      return XSiteNamedCache.cachedByteString(transport.localSiteName());
   }

   private void sendWithBackOff(XSiteBackup backup, XSiteRemoteEventCommand cmd) {
      if (transport.localSiteName().equals(backup.getSiteName())) {
         return;
      }
      new BackOffSender(cmd, backup).run();
   }

   private Executor delayExecutor(int step) {
      return CompletableFuture.delayedExecutor(BACK_OFF_DELAYS[step], TimeUnit.MILLISECONDS, backOffExecutor);
   }

   private MetricsRegistry getMetricsRegistry() {
      return metricsRegistry;
   }

   private Transport getTransport() {
      return transport;
   }

   private boolean isRemoteSite(String siteName) {
      return !Objects.equals(siteName, transport.localSiteName());
   }

   private class BackOffSender implements Runnable, Function<Throwable, Void> {
      private final XSiteRemoteEventCommand cmd;
      private final XSiteBackup backup;
      @GuardedBy("this")
      private int backoffStep;

      private BackOffSender(XSiteRemoteEventCommand cmd, XSiteBackup backup) {
         this.cmd = cmd;
         this.backup = backup;
      }

      @Override
      public void run() {
         log.debugf("Sending %s to %s", cmd, backup);
         transport.backupRemotely(backup, cmd).exceptionally(this);
      }

      @Override
      public Void apply(Throwable throwable) {
         var step = nextBackOffStep();
         if (step >= BACK_OFF_DELAYS.length) {
            log.debugf(throwable, "Failed to send %s to %s", cmd, backup);
            return null;
         }
         log.debugf(throwable, "Sending %s to %s with delay of %s milliseconds", cmd, backup, BACK_OFF_DELAYS[step]);
         delayExecutor(step).execute(this);
         return null;
      }

      private synchronized int nextBackOffStep() {
         return backoffStep++;
      }
   }
}
