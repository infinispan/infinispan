package org.infinispan.server.memcached;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.memcached.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

/**
 * Memcached server defining its decoder/encoder settings. In fact, Memcached does not use an encoder since there's
 * no really common headers between protocol operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedServer extends AbstractProtocolServer<MemcachedServerConfiguration> {
   public MemcachedServer() {
      super("Memcached");
   }

   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private AdvancedCache<String, byte[]> memcachedCache;

   @Override
   protected void startInternal(MemcachedServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      if (cacheManager.getCacheConfiguration(configuration.defaultCacheName()) == null) {
         // Define the Memcached cache as clone of the default one
         cacheManager.defineConfiguration(configuration.defaultCacheName(),
            new ConfigurationBuilder().read(cacheManager.getDefaultCacheConfiguration()).build());
      }
      ExpirationConfiguration expConfig = cacheManager.getCacheConfiguration(configuration.defaultCacheName()).expiration();
      if (expConfig.lifespan() >= 0 || expConfig.maxIdle() >= 0)
        throw log.invalidExpiration(configuration.defaultCacheName());
      Cache<String, byte[]> cache = cacheManager.getCache(configuration.defaultCacheName());
      memcachedCache = cache.getAdvancedCache();

      super.startInternal(configuration, cacheManager);
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return new MemcachedDecoder(memcachedCache, scheduler, transport, this::isCacheIgnored);
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new NettyInitializers(new NettyChannelInitializer<>(this, transport, getEncoder(), getDecoder()));
   }

   @Override
   public void stop() {
      super.stop();
      scheduler.shutdown();
   }

   @Override
   public int getWorkerThreads() {
      return Integer.getInteger("infinispan.server.memcached.workerThreads", configuration.workerThreads());
   }
}
