package org.infinispan.server.resp;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;

/**
 * Server that supports RESP protocol
 *
 * @author William Burns
 * @since 14.0
 */
public class RespServer extends AbstractProtocolServer<RespServerConfiguration> {
   public static final String RESP_SERVER_FEATURE = "resp-server";
   private final static Log log = LogFactory.getLog(RespServer.class, Log.class);

   public RespServer() {
      super("Resp");
   }

   @Override
   protected void startInternal() {
      if (!cacheManager.getCacheManagerConfiguration().features().isAvailable(RESP_SERVER_FEATURE)) {
         throw CONFIG.featureDisabled(RESP_SERVER_FEATURE);
      }
      if (cacheManager.getCacheConfiguration(configuration.defaultCacheName()) == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         Configuration defaultCacheConfiguration = cacheManager.getDefaultCacheConfiguration();
         if (defaultCacheConfiguration != null) { // We have a default configuration, use that
            builder.read(defaultCacheConfiguration);
         } else {
            if (cacheManager.getCacheManagerConfiguration().isClustered()) { // We are running in clustered mode
               builder.clustering().cacheMode(CacheMode.REPL_SYNC);
            }
            builder.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
            builder.encoding().value().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
         }
         cacheManager.defineConfiguration(configuration.defaultCacheName(), builder.build());
      }
      ExpirationConfiguration expConfig = cacheManager.getCacheConfiguration(configuration.defaultCacheName()).expiration();
      if (expConfig.lifespan() >= 0 || expConfig.maxIdle() >= 0)
        throw log.invalidExpiration(configuration.defaultCacheName());
      super.startInternal();
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return null;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new NettyInitializers(new NettyChannelInitializer<>(this, transport, null, null),
            new RespChannelInitializer(this));
   }

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> channel.pipeline().get(RespLettuceHandler.class) != null;
   }

   @Override
   public void stop() {
      super.stop();
   }

   /**
    * Returns the cache being used by the Resp server
    */
   public AdvancedCache<byte[], byte[]> getCache() {
      return cacheManager.<byte[], byte[]>getCache(configuration.defaultCacheName()).getAdvancedCache();
   }

   public Resp3Handler newHandler() {
      return new Resp3Handler(this);
   }
}
