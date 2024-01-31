package org.infinispan.server.resp;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.iteration.DefaultIterationManager;
import org.infinispan.server.iteration.ExternalSourceIterationManager;
import org.infinispan.server.resp.commands.cluster.SegmentSlotRelation;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.filter.ComposedFilterConverterFactory;
import org.infinispan.server.resp.filter.GlobMatchFilterConverterFactory;
import org.infinispan.server.resp.filter.RespTypeFilterConverterFactory;

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
   private static final Log log = LogFactory.getLog(RespServer.class);
   public static final String RESP_SERVER_FEATURE = "resp-server";
   public static final MediaType RESP_KEY_MEDIA_TYPE = MediaType.APPLICATION_OCTET_STREAM;
   private MediaType configuredValueType = MediaType.APPLICATION_OCTET_STREAM;
   private DefaultIterationManager iterationManager;
   private ExternalSourceIterationManager dataStructureIterationManager;
   private TimeService timeService;
   private SegmentSlotRelation segmentSlots;

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
      iterationManager.addKeyValueFilterConverterFactory(RespTypeFilterConverterFactory.class.getName(), new RespTypeFilterConverterFactory());
      iterationManager.addKeyValueFilterConverterFactory(ComposedFilterConverterFactory.class.getName(), new ComposedFilterConverterFactory());
      dataStructureIterationManager.addKeyValueFilterConverterFactory(GlobMatchFilterConverterFactory.class.getName(), new GlobMatchFilterConverterFactory(true));
      if (!cacheManager.getCacheManagerConfiguration().features().isAvailable(RESP_SERVER_FEATURE)) {
         throw CONFIG.featureDisabled(RESP_SERVER_FEATURE);
      }
      String cacheName = configuration.defaultCacheName();
      Configuration explicitConfiguration = cacheManager.getCacheConfiguration(cacheName);
      if (explicitConfiguration == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         Configuration defaultCacheConfiguration = cacheManager.getDefaultCacheConfiguration();
         if (defaultCacheConfiguration != null) { // We have a default configuration, use that
            builder.read(defaultCacheConfiguration);
            configuredValueType = builder.encoding().value().mediaType();
            if (cacheManager.getCacheManagerConfiguration().isClustered() &&
                  !(builder.clustering().hash().keyPartitioner() instanceof RESPHashFunctionPartitioner)) {
               throw CONFIG.respCacheUseDefineConsistentHash(cacheName, builder.clustering().hash().keyPartitioner().getClass().getName());
            }
            MediaType keyMediaType = builder.encoding().key().mediaType();
            if (keyMediaType == null) {
               log.debugf("Setting RESP cache key media type storage to OCTET stream to avoid key encodings");
               builder.encoding().key().mediaType(RESP_KEY_MEDIA_TYPE);
            } else if (!keyMediaType.equals(RESP_KEY_MEDIA_TYPE)) {
               throw CONFIG.respCacheKeyMediaTypeSupplied(cacheName, keyMediaType);
            }
         } else {
            if (cacheManager.getCacheManagerConfiguration().isClustered()) { // We are running in clustered mode
               builder.clustering().cacheMode(CacheMode.REPL_SYNC);
               // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
               builder.clustering().hash().keyPartitioner(new RESPHashFunctionPartitioner());
            }
            builder.encoding().key().mediaType(RESP_KEY_MEDIA_TYPE);
            builder.encoding().value().mediaType(configuredValueType);
            builder.statistics().enable();
         }
         Configuration cfg = builder.build();
         cacheManager.defineConfiguration(configuration.defaultCacheName(), cfg);
         segmentSlots = new SegmentSlotRelation(cfg.clustering().hash().numSegments());
      } else {
         if (!RESP_KEY_MEDIA_TYPE.equals(explicitConfiguration.encoding().keyDataType().mediaType()))
            throw CONFIG.respCacheKeyMediaTypeSupplied(cacheName, explicitConfiguration.encoding().keyDataType().mediaType());

         if (cacheManager.getCacheManagerConfiguration().isClustered() &&
               !(explicitConfiguration.clustering().hash().keyPartitioner() instanceof RESPHashFunctionPartitioner)) {
            throw CONFIG.respCacheUseDefineConsistentHash(cacheName, explicitConfiguration.clustering().hash().keyPartitioner().getClass().getName());
         }
         segmentSlots = new SegmentSlotRelation(explicitConfiguration.clustering().hash().numSegments());
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

   public SegmentSlotRelation segmentSlotRelation() {
      return segmentSlots;
   }
}
