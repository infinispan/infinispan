package org.infinispan.manager.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Tracks the {@link org.infinispan.commons.util.Version} of {@link org.infinispan.Cache} containers running on each
 * member of the cluster.
 * <p>
 * The versions are collected with the same mechanism used by the {@code GET /v2/cluster/members} REST endpoint, i.e. by
 * aggregating the {@link org.infinispan.manager.CacheManagerInfo} of every member. Because JMX attributes are read
 * synchronously and are usually polled by consoles, the result of the aggregation is cached: it is refreshed
 * asynchronously whenever the cluster view changes and whenever the cached value is older than
 * {@link #REFRESH_INTERVAL_MILLIS}. Members that do not answer the query are reported as {@link #UNKNOWN_VERSION}.
 *
 * @since 16.4
 */
@Scope(Scopes.GLOBAL)
@Listener
public class ClusterMemberVersions {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   /**
    * Reported for a member which did not answer the version query, e.g. because it is still starting, because it is
    * unreachable, or because it runs a version which cannot execute the query.
    */
   public static final String UNKNOWN_VERSION = "unknown";

   /**
    * How long a collected value is served before a new cluster wide query is triggered.
    */
   static final long REFRESH_INTERVAL_MILLIS = 30_000;

   /**
    * How long a value which is missing the version of at least one member is served before a new cluster wide query is
    * triggered. A member which joined the view but is not ready to answer requests yet, or a member which left before
    * answering, must not be reported as unknown until the next regular refresh.
    */
   static final long INCOMPLETE_REFRESH_INTERVAL_MILLIS = 5_000;

   /**
    * How long a cluster wide query waits for the slowest member.
    */
   static final long QUERY_TIMEOUT_MILLIS = 10_000;

   /**
    * How long a read waits for the very first cluster wide query to complete. Subsequent reads are always served from
    * the cached value.
    */
   static final long INITIAL_READ_TIMEOUT_MILLIS = 1_000;

   private final EmbeddedCacheManager cacheManager;
   private final AtomicReference<Versions> versions = new AtomicReference<>();
   private final AtomicReference<CompletableFuture<Versions>> pendingRefresh = new AtomicReference<>();

   @Inject CacheManagerNotifier notifier;
   @Inject BlockingManager blockingManager;
   @Inject TimeService timeService;

   public ClusterMemberVersions(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Start
   void start() {
      notifier.addListener(this);
   }

   @Stop
   void stop() {
      notifier.removeListener(this);
   }

   @Merged
   @ViewChanged
   @SuppressWarnings("unused")
   public void onViewChange(ViewChangedEvent event) {
      // The view is installed by the JGroups view handler thread, which also waits for the listeners to complete,
      // so the refresh must not be performed inline.
      blockingManager.runBlocking(this::refresh, "cluster-member-versions-refresh");
   }

   /**
    * Returns the Infinispan version of each cluster member, keyed by the member's logical address. A member that did
    * not answer the version query is reported as {@link #UNKNOWN_VERSION}.
    * <p>
    * The value is served from a cache that is refreshed asynchronously, so this method does not trigger a cluster wide
    * invocation on every call. Only the very first call may block, for at most
    * {@link #INITIAL_READ_TIMEOUT_MILLIS} milliseconds, while the first query is in flight.
    *
    * @return an immutable map of member address to Infinispan version, in cluster view order.
    */
   public Map<String, String> getClusterMemberVersions() {
      Transport transport = transport();
      if (transport == null) {
         return Map.of(cacheManager.getCacheManagerInfo().getNodeAddress(), cacheManager.getCacheManagerInfo().getVersion());
      }
      Versions current = currentVersions(transport);
      Map<Address, String> collected = current == null ? Map.of() : current.versions();
      Map<String, String> result = new LinkedHashMap<>();
      for (Address member : transport.getMembers()) {
         result.put(member.toString(), collected.getOrDefault(member, UNKNOWN_VERSION));
      }
      return Collections.unmodifiableMap(result);
   }

   /**
    * @return {@code true} if the cluster members do not all run the same Infinispan version, i.e. a rolling upgrade is
    * in progress.
    */
   public boolean isRollingUpgrade() {
      return isRollingUpgrade(getClusterMemberVersions().values());
   }

   /**
    * Mirrors {@code ClusterResource#isRollingUpgrade}: members whose version could not be determined are ignored,
    * otherwise a single unreachable member would be reported as a rolling upgrade.
    */
   static boolean isRollingUpgrade(Collection<String> memberVersions) {
      String firstVersion = null;
      for (String version : memberVersions) {
         if (UNKNOWN_VERSION.equals(version)) {
            continue;
         }
         if (firstVersion == null) {
            firstVersion = version;
         } else if (!firstVersion.equals(version)) {
            return true;
         }
      }
      return false;
   }

   private Versions currentVersions(Transport transport) {
      Versions current = versions.get();
      if (!isStale(current, transport)) {
         return current;
      }
      CompletableFuture<Versions> refresh = refresh();
      if (current != null) {
         // Serve the previous value, the refreshed one will be picked up by the next read.
         return current;
      }
      try {
         if (!CompletableFutures.await(refresh, INITIAL_READ_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            log.debugf("Timed out collecting the Infinispan version of the cluster members");
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return versions.get();
   }

   private boolean isStale(Versions current, Transport transport) {
      return current == null || current.viewId() != transport.getViewId()
            || timeService.isTimeExpired(current.expirationTimeNanos());
   }

   private CompletableFuture<Versions> refresh() {
      Transport transport = transport();
      if (transport == null) {
         return CompletableFutures.completedNull();
      }
      CompletableFuture<Versions> inFlight = pendingRefresh.get();
      if (inFlight != null) {
         return inFlight;
      }
      CompletableFuture<Versions> refresh = new CompletableFuture<>();
      if (!pendingRefresh.compareAndSet(null, refresh)) {
         CompletableFuture<Versions> concurrent = pendingRefresh.get();
         return concurrent != null ? concurrent : CompletableFuture.completedFuture(versions.get());
      }
      try {
         collectVersions(transport, refresh);
      } catch (Throwable t) {
         log.debugf(t, "Unable to collect the Infinispan version of the cluster members");
         complete(refresh, versions.get());
      }
      return refresh;
   }

   private void collectVersions(Transport transport, CompletableFuture<Versions> refresh) {
      int viewId = transport.getViewId();
      int memberCount = transport.getMembers().size();
      Address localAddress = transport.getAddress();
      Map<Address, String> collected = new ConcurrentHashMap<>();
      SecurityActions.getClusterExecutor(cacheManager)
            .timeout(QUERY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            .submitConsumer(ClusterMemberVersions::localVersion, (address, version, throwable) -> {
               if (throwable != null) {
                  log.debugf(throwable, "Unable to retrieve the Infinispan version of %s", address);
                  return;
               }
               // The cluster executor reports a null address for the local node when the transport is not available.
               Address member = address != null ? address : localAddress;
               if (member != null && version != null) {
                  collected.put(member, version);
               }
            })
            .whenComplete((ignore, throwable) -> {
               if (throwable != null) {
                  log.debugf(throwable, "Unable to retrieve the Infinispan version of every cluster member");
               }
               complete(refresh, new Versions(viewId, expirationTime(collected.size() >= memberCount),
                     Map.copyOf(collected)));
            });
   }

   private void complete(CompletableFuture<Versions> refresh, Versions result) {
      if (result != null) {
         versions.set(result);
      }
      pendingRefresh.set(null);
      refresh.complete(result);
   }

   private long expirationTime(boolean everyMemberAnswered) {
      long interval = everyMemberAnswered ? REFRESH_INTERVAL_MILLIS : INCOMPLETE_REFRESH_INTERVAL_MILLIS;
      return timeService.expectedEndTime(interval, TimeUnit.MILLISECONDS);
   }

   private Transport transport() {
      return SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(Transport.class);
   }

   private static String localVersion(EmbeddedCacheManager cacheManager) {
      return cacheManager.getCacheManagerInfo().getVersion();
   }

   private record Versions(int viewId, long expirationTimeNanos, Map<Address, String> versions) {
   }
}
