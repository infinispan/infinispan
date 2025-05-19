package org.infinispan.rest.cachemanager;

import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.rest.distribution.CacheDistributionInfo;
import org.infinispan.rest.distribution.CompleteKeyDistribution;
import org.infinispan.rest.distribution.KeyDistributionInfo;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.server.core.CacheInfo;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.CacheSpanAttribute;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.logging.LogFactory;

/**
 * Manages caches instances used during rest requests.
 */
public class RestCacheManager<V> {

   protected static final Log logger = LogFactory.getLog(RestCacheManager.class, Log.class);

   private final EmbeddedCacheManager instance;
   private final InternalCacheRegistry icr;
   private final Predicate<? super String> isCacheIgnored;
   private final boolean allowInternalCacheAccess;
   private final Map<String, CacheInfo<Object, V>> knownCaches = new ConcurrentHashMap<>(4, 0.9f, 16);
   private final RemoveCacheListener removeCacheListener;
   private final Authorizer authorizer;

   public RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.icr = SecurityActions.getGlobalComponentRegistry(instance).getComponent(InternalCacheRegistry.class);
      this.authorizer = SecurityActions.getGlobalComponentRegistry(instance).getComponent(Authorizer.class);
      this.allowInternalCacheAccess = SecurityActions.getCacheManagerConfiguration(instance)
            .security().authorization().enabled();

      removeCacheListener = new RemoveCacheListener();
      SecurityActions.addListener(instance, removeCacheListener);
   }

   public AdvancedCache<Object, V> getCache(String name, MediaType keyContentType, MediaType valueContentType, RestRequest request) {
      Subject subject = request.getSubject();
      Flag[] flags = request.getFlags();
      if (isCacheIgnored.test(name)) {
         throw logger.cacheUnavailable(name);
      }
      if (keyContentType == null || valueContentType == null) {
         throw logger.missingRequiredMediaType(name);
      }
      checkCacheAvailable(name);
      CacheInfo<Object, V> cacheInfo = knownCaches.get(name);
      if (cacheInfo == null) {
         AdvancedCache<Object, V> cache = instance.<Object, V>getCache(name).getAdvancedCache()
               .withFlags(Flag.IGNORE_RETURN_VALUES);
         cacheInfo = new CacheInfo<>(cache);
         knownCaches.putIfAbsent(name, cacheInfo);
      }
      AdvancedCache<Object, V> cache = cacheInfo.getCache(new KeyValuePair<>(keyContentType, valueContentType), subject);
      if (flags != null && flags.length > 0) cache = cache.withFlags(flags);
      return cache;
   }

   public AdvancedCache<Object, V> getCache(String name, MediaType keyContentType, RestRequest request) {
      if (keyContentType == null)
         return getCache(name, request);

      return getCache(name, keyContentType, MATCH_ALL, request);
   }

   public AdvancedCache<Object, V> getCache(String name, RestRequest restRequest) {
      return getCache(name, MATCH_ALL, MATCH_ALL, restRequest);
   }

   public boolean cacheExists(String name) {
      return instance.cacheExists(name);
   }

   private void checkCacheAvailable(String cacheName) {
      if (!instance.isRunning(cacheName)) {
         GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(instance);
         LocalTopologyManager ltm = gcr.getLocalTopologyManager();
         if (ltm != null) ltm.assertTopologyStable(cacheName);

         throw logger.cacheNotFound(cacheName);
      } else if (icr.isInternalCache(cacheName)) {
         if (icr.isPrivateCache(cacheName)) {
            throw logger.requestNotAllowedToInternalCaches(cacheName);
         } else if (!allowInternalCacheAccess && !icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.USER)) {
            throw logger.requestNotAllowedToInternalCachesWithoutAuthz(cacheName);
         }
      }
   }

   public boolean isCacheQueryable(Cache<?, ?> cache) {
      return SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache())
            .getComponent(StorageConfigurationManager.class)
            .isQueryable();
   }

   public Collection<String> getCacheNames() {
      return instance.getCacheNames();
   }

   public Collection<String> getAccessibleCacheNames() {
      return instance.getAccessibleCacheNames();
   }

   public CompletionStage<CacheEntry<Object, V>> getInternalEntry(String cacheName, Object key, MediaType keyContentType, MediaType mediaType, RestRequest request) {
      return getInternalEntry(cacheName, key, false, keyContentType, mediaType, request);
   }

   public CompletionStage<V> remove(String cacheName, Object key, MediaType keyContentType, RestRequest restRequest) {
      Cache<Object, V> cache = getCache(cacheName, keyContentType, MediaType.MATCH_ALL, restRequest);
      return cache.removeAsync(key);
   }

   public CompletionStage<CacheEntry<Object, V>> getPrivilegedInternalEntry(AdvancedCache<Object, V> cache, Object key, boolean skipListener) {
      AdvancedCache<Object, V> cacheSkip = skipListener ? cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : cache;
      return SecurityActions.getCacheEntryAsync(cacheSkip, key);
   }

   private CompletionStage<CacheEntry<Object, V>> getInternalEntry(AdvancedCache<Object, V> cache, Object key, boolean skipListener) {
      return skipListener ? cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntryAsync(key) : cache.getCacheEntryAsync(key);
   }

   public MediaType getValueConfiguredFormat(String cacheName, RestRequest restRequest) {
      return SecurityActions.getCacheConfiguration(getCache(cacheName, restRequest)).encoding().valueDataType().mediaType();
   }

   private CompletionStage<CacheEntry<Object, V>> getInternalEntry(String cacheName, Object key, boolean skipListener, MediaType keyContentType, MediaType mediaType, RestRequest restRequest) {
      return getInternalEntry(getCache(cacheName, keyContentType, mediaType, restRequest), key, skipListener);
   }

   public String getNodeName() {
      Address addressToBeReturned = instance.getAddress();
      if (addressToBeReturned == null) {
         return "0.0.0.0";
      }
      return addressToBeReturned.toString();
   }

   public String getServerAddress() {
      Transport transport = SecurityActions.getGlobalComponentRegistry(instance).getComponent(Transport.class);
      if (transport instanceof JGroupsTransport) {
         return transport.getPhysicalAddresses().toString();
      }
      return "0.0.0.0";
   }

   public String getPrimaryOwner(String cacheName, Object key, RestRequest restRequest) {
      DistributionManager dm = SecurityActions.getDistributionManager(getCache(cacheName, restRequest));
      if (dm == null) {
         //this is a local cache
         return "0.0.0.0";
      }
      return dm.getCacheTopology().getDistribution(key).primary().toString();
   }

   public String getBackupOwners(String cacheName, Object key, RestRequest restRequest) {
      DistributionManager dm = SecurityActions.getDistributionManager(getCache(cacheName, restRequest));
      if (dm == null) {
         //this is a local cache
         return "0.0.0.0";
      }
      return dm.getCacheTopology().getDistribution(key).writeBackups().stream().map(a -> a.toString()).collect(Collectors.joining(" "));
   }

   public EmbeddedCacheManager getInstance() {
      return instance;
   }

   public Authorizer getAuthorizer() {
      return authorizer;
   }

   public EmbeddedCacheManagerAdmin getCacheManagerAdmin(RestRequest restRequest) {
      Subject subject = restRequest.getSubject();
      if (subject == null) {
         return instance.administration();
      } else {
         return Security.doAs(subject, () -> instance.administration().withSubject(subject));
      }
   }

   public void stop() {
      if (removeCacheListener != null) {
         CompletionStages.join(SecurityActions.removeListenerAsync(instance, removeCacheListener));
      }
   }

   public void resetCacheInfo(String cacheName) {
      knownCaches.remove(cacheName);
   }

   public CompletionStage<List<CacheDistributionInfo>> cacheDistribution(String cacheName, RestRequest request) {
      AdvancedCache<?, ?> ac = getCache(cacheName, request);
      checkCachePermission(ac, request.getSubject(), AuthorizationPermission.MONITOR);
      DistributionManager dm = SecurityActions.getDistributionManager(ac);
      if (dm == null) {
         CacheDistributionInfo local = CacheDistributionInfo.resolve(ac, instance.getCacheManagerInfo());
         return CompletableFuture.completedFuture(Collections.singletonList(local));
      }

      return requestMembers(dm.getCacheTopology().getMembers(), ecm -> {
         Cache<?, ?> c = SecurityActions.getCache(ecm, cacheName);
         return CacheDistributionInfo.resolve(c.getAdvancedCache());
      });
   }

   public CompletionStage<CompleteKeyDistribution> getKeyDistribution(String cacheName, Object key, RestRequest request) {
      AdvancedCache<Object, ?> cache = getCache(cacheName, request.keyContentType(), request);
      checkCachePermission(cache, request.getSubject(), AuthorizationPermission.MONITOR);
      return SecurityActions.cacheContainsKeyAsync(cache, key)
            .thenCompose(contains -> {
               DistributionManager dm = SecurityActions.getDistributionManager(cache);
               if (dm == null) {
                  KeyDistributionInfo local = KeyDistributionInfo.resolve(cache, true);
                  return CompletableFuture.completedFuture(new CompleteKeyDistribution(Collections.singletonList(local), contains));
               }

               LocalizedCacheTopology topology = dm.getCacheTopology();
               List<Address> members = topology.getMembers();
               Address primary = null;
               if (contains) {
                  DistributionInfo distribution = topology.getDistribution(key);
                  members = distribution.readOwners();
                  primary = distribution.primary();
               }

               final Address p = primary;
               return requestMembers(members, ecm -> {
                  Cache<?, ?> c = SecurityActions.getCache(ecm, cacheName);
                  boolean isPrimary = p != null && ecm.getAddress().equals(p);
                  return KeyDistributionInfo.resolve(c.getAdvancedCache(), isPrimary);
               }).thenApply(d -> new CompleteKeyDistribution(d, contains));
            });
   }

   private <T> CompletionStage<List<T>> requestMembers(List<Address> addresses, SerializableFunction<EmbeddedCacheManager, T> function) {
      Map<Address, T> responses = new ConcurrentHashMap<>();
      ClusterExecutor executor = SecurityActions.getClusterExecutor(instance);
      return executor.filterTargets(addresses)
            .submitConsumer(function, (address, res, t) -> {
               if (t != null) {
                  throw CompletableFutures.asCompletionException(t);
               }
               responses.put(address, res);
            }).thenApply(ignore -> Immutables.immutableListConvert(responses.values()));
   }

   private void checkCachePermission(AdvancedCache<?, ?> ac, Subject subject, AuthorizationPermission permission) {
      AuthorizationManager am = SecurityActions.getCacheAuthorizationManager(ac);
      if (am != null) {
         am.checkPermission(subject, permission);
      }
   }

   public InfinispanSpanAttributes getInfinispanSpanAttributes(String cacheName, RestRequest request) {
      var cacheInfo = knownCaches.get(cacheName);
      if (cacheInfo != null) {
         return cacheInfo.getInfinispanSpanAttributes();
      }
      // the code below should never be invoked.
      return SecurityActions.getCacheComponentRegistry(getCache(cacheName, request))
            .getComponent(CacheSpanAttribute.class)
            .getAttributes(SpanCategory.CONTAINER);
   }

   @Listener
   class RemoveCacheListener {
      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         resetCacheInfo(event.getCacheName());
      }

      @ConfigurationChanged
      public void configurationChanged(ConfigurationChangedEvent event) {
         knownCaches.clear(); // TODO: be less aggressive
      }
   }

}
