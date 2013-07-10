package org.infinispan.loaders.remote;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfiguration;
import org.infinispan.loaders.remote.configuration.RemoteServerConfiguration;
import org.infinispan.loaders.remote.logging.Log;
import org.infinispan.loaders.remote.wrapper.HotRodEntryMarshaller;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Cache store that delegates the call to a infinispan cluster. Communication between this cache store and the remote
 * cluster is achieved through the java HotRod client: this assures fault tolerance and smart dispatching of calls to
 * the nodes that have the highest chance of containing the given key. This cache store supports both preloading
 * and <b>fetchPersistentState</b>.
 * <p/>
 * Purging elements is not possible, as HotRod does not support the fetching of all remote keys (this would be a
 * very costly operation as well). Purging takes place at the remote end (infinispan cluster).
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfiguration
 * @see <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 * @since 4.1
 */
@ThreadSafe
public class RemoteCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(RemoteCacheStore.class, Log.class);

   private RemoteCacheStoreConfiguration configuration;

   private volatile RemoteCacheManager remoteCacheManager;
   private volatile RemoteCache<Object, Object> remoteCache;

   private InternalEntryFactory iceFactory;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      if (configuration.rawValues()) {
         MetadataValue<?> value = remoteCache.getWithMetadata(key);
         if (value != null)
            return iceFactory.create(key, value.getValue(), new NumericVersion(value.getVersion()),
                  value.getCreated(), TimeUnit.SECONDS.toMillis(value.getLifespan()),
                  value.getLastUsed(), TimeUnit.SECONDS.toMillis(value.getMaxIdle()));
         else
            return null;
      } else {
         return (InternalCacheEntry) remoteCache.get(key);
      }
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.trace("Skipping purge call, as this is performed on the remote cache.");
      }
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return remoteCache.containsKey(key);
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.tracef("Adding entry: %s", entry);
      }
      remoteCache.put(entry.getKey(), configuration.rawValues() ? entry.getValue() : entry, toSeconds(entry.getLifespan(), entry, LIFESPAN), TimeUnit.SECONDS, toSeconds(entry.getMaxIdle(), entry, MAXIDLE), TimeUnit.SECONDS);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      Map<?, ?> result;
      try {
         result = (Map<Object, InternalCacheEntry>) marshaller.objectFromObjectStream(inputStream);
         remoteCache.putAll(result);
      } catch (Exception e) {
         throw new CacheLoaderException("Exception while reading data", e);
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      Map<?, ?> map = remoteCache.getBulk();
      try {
         marshaller.objectToObjectStream(map, outputStream);
      } catch (IOException e) {
         throw new CacheLoaderException("Exception while serializing remote data to stream", e);
      }
   }

   @Override
   public void clear() throws CacheLoaderException {
      remoteCache.clear();
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      // Less than ideal, but RemoteCache, since it extends Cache, can only
      // know whether the operation succeded based on whether the previous
      // value is null or not.
      return remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(key) != null;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Map<Object, Object> map = remoteCache.getBulk();
      return convertToInternalCacheEntries(map);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return convertToInternalCacheEntries(remoteCache.getBulk(numEntries));
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      log.sharedModeOnlyAllowed();
      throw new CacheLoaderException("RemoteCacheStore can only run in shared mode! This method shouldn't be called in shared mode");
   }

   @Override
   public void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache, StreamingMarshaller m) throws
         CacheLoaderException {
      if (configuration instanceof RemoteCacheStoreConfiguration) {
         this.configuration = (RemoteCacheStoreConfiguration) configuration;
      } else {
         throw new CacheLoaderException("Incompatible configuration bean passed. Has to be an instance of " +
               RemoteCacheStoreConfiguration.class.getName());
      }
      super.init(configuration, cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      final Marshaller marshaller;
      if (configuration.marshaller() != null) {
         marshaller = Util.getInstance(configuration.marshaller(), cache.getCacheConfiguration().classLoader());
      } else if (configuration.hotRodWrapping()) {
         marshaller = new HotRodEntryMarshaller();
      } else if (configuration.rawValues()) {
         marshaller = new GenericJBossMarshaller();
      } else {
         marshaller = getMarshaller();
      }
      ConfigurationBuilder builder = buildRemoteConfiguration(configuration, marshaller);
      remoteCacheManager = new RemoteCacheManager(builder.build());

      if (configuration.remoteCacheName().equals(BasicCacheContainer.DEFAULT_CACHE_NAME))
         remoteCache = remoteCacheManager.getCache();
      else
         remoteCache = remoteCacheManager.getCache(configuration.remoteCacheName());
      if (configuration.rawValues() && iceFactory == null) {
         iceFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      remoteCacheManager.stop();
   }

   private long toSeconds(long millis, InternalCacheEntry entry, String desc) {
      if (millis > 0 && millis < 1000) {
         if (log.isTraceEnabled()) {
            log.tracef("Adjusting %s time for (k,v): (%s, %s) from %d millis to 1 sec, as milliseconds are not supported by HotRod",
                       desc ,entry.getKey(), entry.getValue(), millis);
         }
         return 1;
      }
      return TimeUnit.MILLISECONDS.toSeconds(millis);
   }

   private Set<InternalCacheEntry> convertToInternalCacheEntries(Map<Object, Object> map) throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(map.size());
      Set<Map.Entry<Object, Object>> set = map.entrySet();
      for (Map.Entry<Object, Object> e : set) {
         if (configuration.rawValues()) {
            result.add(load(e.getKey())); // Inefficient: should probably have a getBulkWithMetadata
         } else {
            result.add((InternalCacheEntry) e.getValue());
         }
      }
      return result;
   }

   public void setInternalCacheEntryFactory(InternalEntryFactory iceFactory) {
      if (this.iceFactory != null) {
         throw new IllegalStateException();
      }
      this.iceFactory = iceFactory;
   }

   public RemoteCache<Object, Object> getRemoteCache() {
      return remoteCache;
   }

   private ConfigurationBuilder buildRemoteConfiguration(RemoteCacheStoreConfiguration configuration, Marshaller marshaller) {
      ConfigurationBuilder builder = new ConfigurationBuilder();

      for (RemoteServerConfiguration s : configuration.servers()) {
         builder.addServer()
               .host(s.host())
               .port(s.port());
      }

      ConnectionPoolConfiguration poolConfiguration = configuration.connectionPool();
      Long connectionTimeout = configuration.connectionTimeout();
      Long socketTimeout = configuration.socketTimeout();

      builder.balancingStrategy(configuration.balancingStrategy())
            .connectionPool()
            .exhaustedAction(ExhaustedAction.valueOf(poolConfiguration.exhaustedAction().toString()))
            .maxActive(poolConfiguration.maxActive())
            .maxIdle(poolConfiguration.maxIdle())
            .maxTotal(poolConfiguration.maxTotal())
            .minIdle(poolConfiguration.minIdle())
            .minEvictableIdleTime(poolConfiguration.minEvictableIdleTime())
            .testWhileIdle(poolConfiguration.testWhileIdle())
            .timeBetweenEvictionRuns(poolConfiguration.timeBetweenEvictionRuns())
            .connectionTimeout(connectionTimeout.intValue())
            .forceReturnValues(configuration.forceReturnValues())
            .keySizeEstimate(configuration.keySizeEstimate())
            .marshaller(marshaller)
            .asyncExecutorFactory().factoryClass(configuration.asyncExecutorFactory().factory().getClass())
            .classLoader(configuration.getClass().getClassLoader())
            .pingOnStartup(configuration.pingOnStartup())
            .socketTimeout(socketTimeout.intValue())
            .tcpNoDelay(configuration.tcpNoDelay())
            .valueSizeEstimate(configuration.valueSizeEstimate());
      if (configuration.protocolVersion() != null)
         builder.protocolVersion(configuration.protocolVersion());
      if (configuration.transportFactory() != null)
         builder.transportFactory(configuration.transportFactory());

      return builder;
   }
}
