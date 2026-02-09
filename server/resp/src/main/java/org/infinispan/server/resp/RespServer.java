package org.infinispan.server.resp;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.iteration.DefaultIterationManager;
import org.infinispan.server.core.iteration.ExternalSourceIterationManager;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.resp.commands.cluster.SegmentSlotRelation;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.filter.ComposedFilterConverterFactory;
import org.infinispan.server.resp.filter.GlobMatchFilterConverterFactory;
import org.infinispan.server.resp.filter.RespTypeFilterConverterFactory;
import org.infinispan.server.resp.meta.MetadataRepository;
import org.infinispan.server.resp.scripting.LuaTaskEngine;
import org.infinispan.tasks.manager.TaskManager;
import org.infinispan.transaction.LockingMode;

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
   private Configuration defaultCacheConfiguration;
   private MetadataRepository metadataRepository;
   private MediaType configuredValueType = MediaType.APPLICATION_OCTET_STREAM;
   private DefaultIterationManager iterationManager;
   private ExternalSourceIterationManager dataStructureIterationManager;
   private TimeService timeService;
   private SegmentSlotRelation segmentSlots;
   private LuaTaskEngine luaTaskEngine;
   private final Random random = new Random(); // TODO: we should be able to set a cluster-wide seed

   public RespServer() {
      super("Resp");
   }

   @Override
   protected void internalPostStart() {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.timeService = gcr.getTimeService();
      this.iterationManager = new DefaultIterationManager(gcr.getTimeService());
      this.dataStructureIterationManager = new ExternalSourceIterationManager(gcr.getTimeService());
      iterationManager.addKeyValueFilterConverterFactory(GlobMatchFilterConverterFactory.class.getName(), new GlobMatchFilterConverterFactory());
      iterationManager.addKeyValueFilterConverterFactory(RespTypeFilterConverterFactory.class.getName(), new RespTypeFilterConverterFactory());
      iterationManager.addKeyValueFilterConverterFactory(ComposedFilterConverterFactory.class.getName(), new ComposedFilterConverterFactory());
      dataStructureIterationManager.addKeyValueFilterConverterFactory(GlobMatchFilterConverterFactory.class.getName(), new GlobMatchFilterConverterFactory(true));
      metadataRepository = new MetadataRepository();
      initializeLuaTaskEngine(gcr);
      defineCacheConfiguration();

      super.internalPostStart();
   }

   private void defineCacheConfiguration() {
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      if (!globalConfiguration.features().isAvailable(RESP_SERVER_FEATURE)) {
         throw CONFIG.featureDisabled(RESP_SERVER_FEATURE);
      }
      String cacheName = configuration.defaultCacheName();
      Configuration explicitConfiguration = SecurityActions.getCacheConfiguration(cacheManager, cacheName);
      if (explicitConfiguration == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         Configuration defaultCacheConfiguration = SecurityActions.getDefaultCacheConfiguration(cacheManager);
         if (defaultCacheConfiguration != null) { // We have a default configuration, use that
            builder.read(defaultCacheConfiguration);
            configuredValueType = builder.encoding().value().mediaType();
            if (globalConfiguration.isClustered() &&
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

            if (builder.transaction().transactionMode().isTransactional()
                  && builder.transaction().lockingMode() != LockingMode.PESSIMISTIC) {
               org.infinispan.server.resp.logging.Log.CONFIG.utilizePessimisticLocking(builder.transaction().lockingMode().name());
            }

         } else {
            if (globalConfiguration.isClustered()) { // We are running in clustered mode
               builder.clustering().cacheMode(CacheMode.DIST_SYNC);
               // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
               builder.clustering().hash().keyPartitioner(new RESPHashFunctionPartitioner());
            }
            builder.encoding().key().mediaType(RESP_KEY_MEDIA_TYPE);
            builder.encoding().value().mediaType(configuredValueType);
         }
         builder.statistics().enable();
         if (!cacheManager.cacheConfigurationExists(RespServerConfiguration.DEFAULT_RESP_CACHE_ALIAS))
            builder.aliases(RespServerConfiguration.DEFAULT_RESP_CACHE_ALIAS);

         explicitConfiguration = builder.build();
         SecurityActions.defineConfiguration(cacheManager, cacheName, explicitConfiguration);
      } else {
         if (!RESP_KEY_MEDIA_TYPE.equals(explicitConfiguration.encoding().keyDataType().mediaType()))
            throw CONFIG.respCacheKeyMediaTypeSupplied(cacheName, explicitConfiguration.encoding().keyDataType().mediaType());

         if (globalConfiguration.isClustered() &&
               !(explicitConfiguration.clustering().hash().keyPartitioner() instanceof RESPHashFunctionPartitioner)) {
            throw CONFIG.respCacheUseDefineConsistentHash(cacheName, explicitConfiguration.clustering().hash().keyPartitioner().getClass().getName());
         }
      }

      defaultCacheConfiguration = explicitConfiguration;
   }

   @Override
   public CompletionStage<Void> initializeDefaultCache() {
      Objects.requireNonNull(defaultCacheConfiguration, "Cache configuration not initialized");
      String cacheName = configuration.defaultCacheName();
      segmentSlots = new SegmentSlotRelation(defaultCacheConfiguration.clustering().hash().numSegments());
      return getBlockingManager()
            // Ensure it invokes `.getOrCreateCache` because it starts the cache in all nodes in the cluster.
            // Otherwise, the node would start only at the node the connection was established.
            .runBlocking(() -> SecurityActions.getOrCreateCache(cacheManager, cacheName, defaultCacheConfiguration), "create-resp-cache");
   }

   @Override
   protected void startCaches() { }

   // To be replaced for svm
   private void initializeLuaTaskEngine(GlobalComponentRegistry gcr) {
      // Register the task engine with the task manager
      ScriptingManager scriptingManager = gcr.getComponent(ScriptingManager.class);
      luaTaskEngine = new LuaTaskEngine(scriptingManager);
      TaskManager taskManager = gcr.getComponent(TaskManager.class);
      taskManager.registerTaskEngine(luaTaskEngine);
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

   public LuaTaskEngine luaEngine() {
      return luaTaskEngine;
   }

   @Override
   public void stop() {
      super.stop();
      if (luaTaskEngine != null) {
         luaTaskEngine.shutdown();
      }
   }

   /**
    * Returns the cache being used by the Resp server
    */
   public AdvancedCache<byte[], byte[]> getCache() {
      if (!isDefaultCacheRunning())
         throw new IllegalStateException("Cache is not initialized");
      return cacheManager.<byte[], byte[]>getCache(configuration.defaultCacheName()).getAdvancedCache();
   }

   public Resp3Handler newHandler(AdvancedCache<byte[], byte[]> cache) {
      return new Resp3Handler(this, configuredValueType, cache);
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
      if (segmentSlots == null) {
         String cacheName = configuration.defaultCacheName();
         Configuration explicitConfiguration = SecurityActions.getCacheConfiguration(cacheManager, cacheName);
         segmentSlots = new SegmentSlotRelation(explicitConfiguration.clustering().hash().numSegments());
      }
      return segmentSlots;
   }

   public MetadataRepository metadataRepository() {
      return metadataRepository;
   }

   public Random random() {
      return random;
   }

   @Override
   protected String protocolType() {
      return "redis";
   }

   @Override
   public String toString() {
      return toString("RESP",  "auth=RESP");
   }
}
