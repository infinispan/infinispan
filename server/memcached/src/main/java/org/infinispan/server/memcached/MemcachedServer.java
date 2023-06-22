package org.infinispan.server.memcached;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.memcached.binary.BinaryAuthDecoderImpl;
import org.infinispan.server.memcached.binary.BinaryOpDecoderImpl;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.memcached.text.TextAuthDecoderImpl;
import org.infinispan.server.memcached.text.TextOpDecoderImpl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;

/**
 * Memcached server defining its decoder/encoder settings. In fact, Memcached does not use an encoder since there's no
 * really common headers between protocol operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedServer extends AbstractProtocolServer<MemcachedServerConfiguration> {

   protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

   private Cache<Object, Object> memcachedCache;

   private MemcachedStats statistics;

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
         builder.encoding().key().mediaType(MediaType.TEXT_PLAIN);
         builder.encoding().value().mediaType(MediaType.APPLICATION_OCTET_STREAM);
         cacheManager.defineConfiguration(configuration.defaultCacheName(), builder.build());
      }
      memcachedCache = cacheManager.getCache(configuration.defaultCacheName());
      if (memcachedCache.getCacheConfiguration().statistics().enabled()) {
         statistics = new MemcachedStats();
      }
      super.startInternal();
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   /**
    * Invoked when the Memcached server has a dedicated transport
    */
   @Override
   public ChannelInboundHandler getDecoder() {
      switch (configuration.protocol()) {
         case TEXT:
            if (configuration.authentication().enabled()) {
               return new TextAuthDecoderImpl(this);
            } else {
               return new TextOpDecoderImpl(this);
            }
         case BINARY:
            if (configuration.authentication().enabled()) {
               return new BinaryAuthDecoderImpl(this);
            } else {
               return new BinaryOpDecoderImpl(this);
            }
         default:
            return new MemcachedAutoDetector(this);
      }
   }

   /**
    * Invoked when the Memcached server is part of a single-port router
    */
   public ChannelInboundHandler getDecoder(MemcachedProtocol protocol) {
      switch (protocol) {
         case TEXT:
            if (configuration.authentication().enabled()) {
               return new TextAuthDecoderImpl(this);
            } else {
               return new TextOpDecoderImpl(this);
            }
         case BINARY:
            if (configuration.authentication().enabled()) {
               return new BinaryAuthDecoderImpl(this);
            } else {
               return new BinaryOpDecoderImpl(this);
            }
         default:
            throw new IllegalStateException();
      }
   }

   public void installMemcachedInboundHandler(Channel ch, MemcachedBaseDecoder decoder) {
      MemcachedInboundAdapter inboundAdapter = new MemcachedInboundAdapter(decoder);
      decoder.registerExceptionHandler(inboundAdapter::handleExceptionally);
      ch.pipeline().addLast("handler", inboundAdapter);
   }

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> (channel.pipeline().get(TextOpDecoderImpl.class) != null || channel.pipeline().get(BinaryOpDecoderImpl.class) != null);
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new NettyInitializers(new NettyChannelInitializer<>(this, transport, getEncoder(), null),
            new MemcachedChannelInitializer(this));
   }

   /**
    * This initializer is invoked by the detector
    */
   public ChannelInitializer<Channel> getInitializer(MemcachedProtocol protocol) {
      return new NettyInitializers(new NettyChannelInitializer<>(this, transport, getEncoder(), null),
            new MemcachedChannelInitializer(this, protocol));
   }

   @Override
   public void stop() {
      super.stop();
      scheduler.shutdown();
   }

   /**
    * Returns the cache being used by the Memcached server
    */
   public Cache<Object, Object> getCache() {
      return memcachedCache;
   }

   public ScheduledExecutorService getScheduler() {
      return scheduler;
   }

   @Override
   public void installDetector(Channel ch) {
      switch (configuration.protocol()) {
         case AUTO:
            ch.pipeline()
                  .addLast(MemcachedTextDetector.NAME, new MemcachedTextDetector(this))
                  .addLast(MemcachedBinaryDetector.NAME, new MemcachedBinaryDetector(this));
            break;
         case TEXT:
            ch.pipeline().addLast(MemcachedTextDetector.NAME, new MemcachedTextDetector(this));
            break;
         case BINARY:
            ch.pipeline().addLast(MemcachedBinaryDetector.NAME, new MemcachedBinaryDetector(this));
            break;
      }
   }

   public MemcachedStats getStatistics() {
      return statistics;
   }
}
