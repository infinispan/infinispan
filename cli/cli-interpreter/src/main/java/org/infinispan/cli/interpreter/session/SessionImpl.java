package org.infinispan.cli.interpreter.session;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.util.Collection;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.codec.CodecException;
import org.infinispan.cli.interpreter.codec.CodecRegistry;
import org.infinispan.cli.interpreter.codec.NoneCodec;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.LogFactory;

public class SessionImpl implements Session {
   public static final Log log = LogFactory.getLog(SessionImpl.class, Log.class);
   private static final Codec NO_OP_CODEC = new NoneCodec();

   private final EmbeddedCacheManager cacheManager;
   private final CodecRegistry codecRegistry;
   private final String id;
   private final TimeService timeService;
   private ConfigurationManager configurationManager;
   private Cache<?, ?> cache = null;
   private String cacheName = null;
   private long timestamp;
   private Codec codec;

   public SessionImpl(final CodecRegistry codecRegistry, final EmbeddedCacheManager cacheManager, final String id,
                      TimeService timeService, ConfigurationManager configurationManager) {
      if (timeService == null) {
         throw new IllegalArgumentException("TimeService cannot be null");
      }
      this.codecRegistry = codecRegistry;
      this.cacheManager = cacheManager;
      this.timeService = timeService;
      this.configurationManager = configurationManager;
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
      return (Cache<K, V>) c.getAdvancedCache().withEncoding(IdentityEncoder.class);
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
         configuration = configurationManager.getConfiguration(baseCacheName, true);
         if (configuration == null) {
            throw log.nonExistentCache(baseCacheName);
         }
      } else {
         configuration = cacheManager.getDefaultCacheConfiguration();
         baseCacheName = cacheManager.getCacheManagerConfiguration().defaultCacheName().get();
      }
      if (cacheManager.cacheExists(cacheName)) {
         throw log.cacheAlreadyExists(cacheName);
      }
      if (configuration.clustering().cacheMode().isClustered()) {
         AdvancedCache<?, ?> clusteredCache = cacheManager.getCache(baseCacheName).getAdvancedCache();
         RpcManager rpc = clusteredCache.getRpcManager();
         ComponentRegistry componentRegistry = clusteredCache.getComponentRegistry();
         CommandsFactory factory = componentRegistry.getComponent(CommandsFactory.class);

         CreateCacheCommand ccc = factory.buildCreateCacheCommand(cacheName, baseCacheName);
         try {
            rpc.invokeRemotely(null, ccc, rpc.getSyncRpcOptions());
            ccc.init(componentRegistry, false);
            ccc.invoke();
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
      if (configurationManager.getGlobalConfiguration().defaultCacheName().isPresent())
         resetCache(cacheManager.getCache());
      for (String cacheName : cacheManager.getCacheNames()) {
         resetCache(cacheManager.getCache(cacheName));
      }
      timestamp = timeService.time();
   }

   private void resetCache(final Cache<Object, Object> cache) {
      Configuration configuration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      if (configuration.invocationBatching().enabled()) {
         SecurityActions.endBatch(cache.getAdvancedCache());
      }
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      try {
         if (tm != null && tm.getTransaction() != null) {
            tm.rollback();
         }
      } catch (Exception e) {
         // Ignore the exception
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
      if (isObjectStorage()) return NO_OP_CODEC;
      return codec;
   }

   boolean isObjectStorage() {
      MediaType storageMediaType = cache.getAdvancedCache().getValueDataConversion().getStorageMediaType();
      return APPLICATION_OBJECT.match(storageMediaType);
   }

   @Override
   public Codec getCodec(String codec) throws CodecException {
      Codec c = codecRegistry.getCodec(codec);
      if (c == null) {
         throw log.noSuchCodec(codec);
      } else {
         if (isObjectStorage()) return NO_OP_CODEC;
         return c;
      }
   }

}
