package org.infinispan.xsite.events;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.SiteViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.SitesViewChangedEvent;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteCacheMapper;
import org.infinispan.xsite.XSiteNamedCache;
import org.infinispan.xsite.commands.XSiteLocalEventCommand;
import org.infinispan.xsite.commands.remote.XSiteRemoteEventCommand;

/**
 * Default implementation of {@link XSiteEventsManager}.
 *
 * @since 15.0
 */
@Scope(Scopes.GLOBAL)
@Listener
public class XSiteEventsManagerImpl implements XSiteEventsManager {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject Transport transport;
   @Inject CacheManagerNotifier notifier;
   @Inject GlobalComponentRegistry globalRegistry;
   @Inject XSiteCacheMapper xSiteCacheMapper;

   @Start
   public void start() {
      notifier.addListener(this);
   }

   @Stop
   public void stop() {
      notifier.removeListener(this);
   }

   @Override
   public CompletionStage<Void> onLocalEvents(List<XSiteEvent> events) {
      log.debugf("Local events received: %s", events);
      try (var holder = new XSiteEventSender(transport)) {
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
      log.debugf("On site view changed event: %s", event);
      event.getJoiners().stream()
            .filter(s -> !Objects.equals(s, localSite().toString()))
            .forEach(this::sendNewConnectionEvent);
   }

   @CacheStarted
   public void onCacheStarted(CacheStartedEvent event) {
      log.debugf("On cache started (is coordinator? %s): %s", transport.isCoordinator(), event.getCacheName());
      if (!transport.isCoordinator()) {
         return;
      }
      try (var sender = new XSiteEventSender(transport)) {
         xSiteCacheMapper.findRemoteCachesWithAsyncBackup(event.getCacheName())
               .forEach(i -> sender.addEventToSite(i.siteName(), XSiteEvent.createInitialStateRequest(localSite(), i.cacheName())));
      } catch (Exception e) {
         log.debugf(e, "Unable to send state request for cache %s", event.getCacheName());
      }
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
      transport.backupRemotely(backup, cmd);
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
}
