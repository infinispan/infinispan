package org.infinispan.server.resp;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.hash.CRC16;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.iteration.DefaultIterationManager;
import org.infinispan.server.iteration.ExternalSourceIterationManager;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.filter.GlobMatchFilterConverterFactory;

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
   private MediaType configuredValueType = MediaType.APPLICATION_OCTET_STREAM;
   private DefaultIterationManager iterationManager;
   private ExternalSourceIterationManager dataStructureIterationManager;
   private TimeService timeService;

   public RespServer() {
      super("Resp");
   }

   @Override
   protected void startInternal() {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.timeService = gcr.getTimeService();
      this.iterationManager = new DefaultIterationManager(gcr.getTimeService());
      this.dataStructureIterationManager = new ExternalSourceIterationManager(gcr.getTimeService());
      iterationManager.addKeyValueFilterConverterFactory(GlobMatchFilterConverterFactory.class.getName(), new GlobMatchFilterConverterFactory());
      dataStructureIterationManager.addKeyValueFilterConverterFactory(GlobMatchFilterConverterFactory.class.getName(), new GlobMatchFilterConverterFactory(true));
      if (!cacheManager.getCacheManagerConfiguration().features().isAvailable(RESP_SERVER_FEATURE)) {
         throw CONFIG.featureDisabled(RESP_SERVER_FEATURE);
      }
      if (cacheManager.getCacheConfiguration(configuration.defaultCacheName()) == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         Configuration defaultCacheConfiguration = cacheManager.getDefaultCacheConfiguration();
         if (defaultCacheConfiguration != null) { // We have a default configuration, use that
            builder.read(defaultCacheConfiguration);
            configuredValueType = builder.encoding().value().mediaType();
         } else {
            if (cacheManager.getCacheManagerConfiguration().isClustered()) { // We are running in clustered mode
               builder.clustering().cacheMode(CacheMode.REPL_SYNC);
               builder.clustering().hash().hashFunction(CRC16.getInstance());
            }
            builder.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE);
            builder.encoding().value().mediaType(configuredValueType);
         }
         cacheManager.defineConfiguration(configuration.defaultCacheName(), builder.build());
      }
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
      return channel -> channel.pipeline().get(RespDecoder.class) != null;
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
      return new Resp3Handler(this, configuredValueType);
   }

   @Override
   public void installDetector(Channel ch) {
      ch.pipeline().addLast(RespDetector.NAME, new RespDetector(this));
   }

   public DefaultIterationManager getIterationManager() {
      return iterationManager;
   }

   public ExternalSourceIterationManager getDataStructureIterationManager() {
      return dataStructureIterationManager;
   }

   public TimeService getTimeService() {
      return timeService;
   }
}
