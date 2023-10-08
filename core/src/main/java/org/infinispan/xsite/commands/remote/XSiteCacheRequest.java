package org.infinispan.xsite.commands.remote;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.metrics.XSiteMetricsCollector;

/**
 * A {@link XSiteRequest} which is associated to a cache.
 * <p>
 * The subclass requests have access to the cache's {@link ComponentRegistry} via {@link #invokeInLocalCache(String, ComponentRegistry)}.
 *
 * @since 15.0
 */
public abstract class XSiteCacheRequest<T> implements XSiteRequest<T> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected ByteString cacheName;

   protected XSiteCacheRequest(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   @ProtoField(1)
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public final CompletionStage<T> invokeInLocalSite(String origin, GlobalComponentRegistry registry) {
      var cacheInfo = registry.getXSiteCacheMapper().findLocalCache(origin, cacheName);
      if (cacheInfo == null) {
         // cache does not exist
         // return CacheConfigurationException -> site switched to OFFLINE in the originator and no more retries
         return CompletableFuture.failedFuture(log.xsiteCacheNotFound(origin, cacheName));
      } else if (cacheInfo.isLocalOnly()) {
         // cache exists, but it is not valid to received xsite requests
         // return CacheConfigurationException -> site switched to OFFLINE in the originator and no more retries
         return CompletableFuture.failedFuture(log.xsiteInLocalCache(origin, cacheInfo.cacheName()));
      }
      var cr = registry.getNamedComponentRegistry(cacheInfo.cacheName());
      if (cr == null || !cr.getStatus().allowInvocations()) {
         // XSiteCacheMapper.findLocalCache found a cache but there is no ComponentRegistry. Possible options are
         // * cache is stopped
         // * cache is initializing/starting
         // return IllegalLifecycleStateException -> triggers back-off and retry in the originator
         return CompletableFuture.failedFuture(log.xsiteCacheNotStarted(origin, cacheInfo.cacheName()));
      }
      cr.getComponent(XSiteMetricsCollector.class).recordRequestsReceived(origin);
      return invokeInLocalCache(origin, cr);
   }

   protected abstract CompletionStage<T> invokeInLocalCache(String origin, ComponentRegistry registry);
}
