package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
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

   @Override
   public final CompletionStage<T> invokeInLocalSite(String origin, GlobalComponentRegistry registry) {
      var cacheInfo = registry.getXSiteCacheMapper().findLocalCache(origin, cacheName);
      if (cacheInfo == null) {
         return CompletableFuture.failedFuture(log.xsiteCacheNotFound(origin, cacheName));
      } else if (cacheInfo.isLocalOnly()) {
         return CompletableFuture.failedFuture(log.xsiteInLocalCache(origin, cacheInfo.cacheName()));
      }
      var cr = registry.getNamedComponentRegistry(cacheInfo.cacheName());
      if (cr == null) {
         return CompletableFuture.failedFuture(log.xsiteCacheNotStarted(origin, cacheInfo.cacheName()));
      }
      cr.getComponent(XSiteMetricsCollector.class).recordRequestsReceived(origin);
      return invokeInLocalCache(origin, cr);
   }

   protected abstract CompletionStage<T> invokeInLocalCache(String origin, ComponentRegistry registry);

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      ByteString.writeObject(output, cacheName);
   }

   @Override
   public XSiteRequest<T> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = ByteString.readObject(input);
      return this;
   }
}
