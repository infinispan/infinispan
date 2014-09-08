package org.infinispan.cli.interpreter.session;

import java.util.Collection;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.codec.CodecException;
import org.infinispan.cli.interpreter.codec.CodecRegistry;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

public class SessionImpl implements Session {
   public static final Log log = LogFactory.getLog(SessionImpl.class, Log.class);
   private final EmbeddedCacheManager cacheManager;
   private final CodecRegistry codecRegistry;
   private final String id;
   private final TimeService timeService;
   private Cache<?, ?> cache = null;
   private String cacheName = null;
   private long timestamp;
   private Codec codec;

   public SessionImpl(final CodecRegistry codecRegistry, final EmbeddedCacheManager cacheManager, final String id,
                      TimeService timeService) {
      if (timeService == null) {
         throw new IllegalArgumentException("TimeService cannot be null");
      }
      this.codecRegistry = codecRegistry;
      this.cacheManager = cacheManager;
      this.timeService = timeService;
      this.id = id;
      timestamp = timeService.time();
      codec = this.codecRegistry.getCodec("none");
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   @Override
   public String getId() {
      return id;
   }

   @Override
   public <K, V> Cache<K, V> getCurrentCache() {
      return (Cache<K, V>) cache;
   }

   @Override
   public String getCurrentCacheName() {
      return cacheName;
   }

   @Override
   public <K, V> Cache<K, V> getCache(final String cacheName) {
      Cache<K, V> c;
      if (cacheName != null) {
         c = cacheManager.getCache(cacheName, false);
      } else {
         c = getCurrentCache();
      }
      if (c == null) {
         throw log.nonExistentCache(cacheName);
      }
      return c;
   }

   @Override
   public void setCurrentCache(final String cacheName) {
      cache = getCache(cacheName);
      this.cacheName = cacheName;
   }

   @Override
   public void createCache(String cacheName, String baseCacheName) {
      Configuration configuration;
      if (baseCacheName != null) {
         configuration = cacheManager.getCacheConfiguration(baseCacheName);
         if (configuration == null) {
            throw log.nonExistentCache(baseCacheName);
         }
      } else {
         configuration = cacheManager.getDefaultCacheConfiguration();
         baseCacheName = BasicCacheContainer.DEFAULT_CACHE_NAME;
      }
      if (cacheManager.cacheExists(cacheName)) {
         throw log.cacheAlreadyExists(cacheName);
      }
      if (configuration.clustering().cacheMode().isClustered()) {
         AdvancedCache<?, ?> clusteredCache = cacheManager.getCache(baseCacheName).getAdvancedCache();
         RpcManager rpc = clusteredCache.getRpcManager();
         CommandsFactory factory = clusteredCache.getComponentRegistry().getComponent(CommandsFactory.class);

         CreateCacheCommand ccc = factory.buildCreateCacheCommand(cacheName, baseCacheName);
         StateTransferManager transferManager = clusteredCache.getComponentRegistry().getComponent(StateTransferManager.class);
         try {
            rpc.invokeRemotely(null, ccc, rpc.getDefaultRpcOptions(true));
            ccc.init(cacheManager, transferManager);
            ccc.perform(null);
         } catch (Throwable e) {
            throw log.cannotCreateClusteredCaches(e, cacheName);
         }
      } else {
         ConfigurationBuilder b = new ConfigurationBuilder();
         b.read(configuration);
         cacheManager.defineConfiguration(cacheName, b.build());
         cacheManager.getCache(cacheName);
      }
   }

   @Override
   public void reset() {
      resetCache(cacheManager.getCache());
      for (String cacheName : cacheManager.getCacheNames()) {
         resetCache(cacheManager.getCache(cacheName));
      }
      timestamp = timeService.time();
   }

   private void resetCache(final Cache<Object, Object> cache) {
      Configuration configuration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      if (configuration.invocationBatching().enabled()) {
         cache.endBatch(false);
      }
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      try {
         if (tm.getTransaction() != null) {
            tm.rollback();
         }
      } catch (Exception e) {
      }
   }

   @Override
   public long getTimestamp() {
      return timestamp;
   }

   @Override
   public void setCodec(String codec) throws CodecException {
      this.codec = getCodec(codec);
   }

   @Override
   public Collection<Codec> getCodecs() {
      return codecRegistry.getCodecs();
   }

   @Override
   public Codec getCodec() {
      return codec;
   }

   @Override
   public Codec getCodec(String codec) throws CodecException {
      Codec c = codecRegistry.getCodec(codec);
      if (c == null) {
         throw log.noSuchCodec(codec);
      } else {
         return c;
      }
   }

}
