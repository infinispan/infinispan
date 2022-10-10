package org.infinispan.server.memcached;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.memcached.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;

/**
 * Memcached server defining its decoder/encoder settings. In fact, Memcached does not use an encoder since there's
 * no really common headers between protocol operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedServer extends AbstractProtocolServer<MemcachedServerConfiguration> {

   private final static Log log = LogFactory.getLog(MemcachedServer.class, Log.class);

   protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

   private AdvancedCache<byte[], byte[]> memcachedCache;

   public MemcachedServer() {
      super("Memcached");
   }

   @Override
   protected void startInternal() {
      if (cacheManager.getCacheConfiguration(configuration.defaultCacheName()) == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         Configuration defaultCacheConfiguration = cacheManager.getDefaultCacheConfiguration();
         if (defaultCacheConfiguration != null) { // We have a default configuration, use that
            builder.read(defaultCacheConfiguration);
         } else if (cacheManager.getCacheManagerConfiguration().isClustered()) { // We are running in clustered mode
            builder.clustering().cacheMode(CacheMode.REPL_SYNC);
         }
         cacheManager.defineConfiguration(configuration.defaultCacheName(), builder.build());
      }
      ExpirationConfiguration expConfig = cacheManager.getCacheConfiguration(configuration.defaultCacheName()).expiration();
      if (expConfig.lifespan() >= 0 || expConfig.maxIdle() >= 0)
        throw log.invalidExpiration(configuration.defaultCacheName());
      Cache<byte[], byte[]> cache = cacheManager.getCache(configuration.defaultCacheName());
      memcachedCache = cache.getAdvancedCache();

      super.startInternal();
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return new MemcachedDecoder(memcachedCache, scheduler, transport, this::isCacheIgnored, configuration.clientEncoding());
   }

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> channel.pipeline().get(MemcachedDecoder.class) != null;
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

   /**
    * Returns the cache being used by the Memcached server
    */
   public Cache<byte[], byte[]> getCache() {
      return memcachedCache;
   }
}
