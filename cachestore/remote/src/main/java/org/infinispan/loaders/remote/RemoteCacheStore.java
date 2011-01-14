package org.infinispan.loaders.remote;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.manager.CacheContainer;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
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
 * @see org.infinispan.loaders.remote.RemoteCacheStoreConfig
 * @see <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 * @since 4.1
 */
@ThreadSafe
@CacheLoaderMetadata(configurationClass = RemoteCacheStoreConfig.class)
public class RemoteCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(RemoteCacheStore.class);

   private volatile RemoteCacheStoreConfig config;
   private volatile RemoteCacheManager remoteCacheManager;
   private volatile RemoteCache<Object, Object> remoteCache;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return (InternalCacheEntry) remoteCache.get(key);
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
         log.trace("Adding entry: " + entry);
      }
      remoteCache.put(entry.getKey(), entry, toSeconds(entry.getLifespan(), entry, LIFESPAN), TimeUnit.SECONDS, toSeconds(entry.getMaxIdle(), entry, MAXIDLE), TimeUnit.SECONDS);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      Map result;
      try {
         result = (Map<Object, InternalCacheEntry>) marshaller.objectFromObjectStream(inputStream);
         remoteCache.putAll(result);
      } catch (Exception e) {
         throw new CacheLoaderException("Exception while reading data", e);
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      Map map = remoteCache.getBulk();
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
      return remoteCache.remove(key) != null;
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
      String message = "RemoteCacheStore can only run in shared mode! This method shouldn't be called in shared mode";
      log.error(message);
      throw new CacheLoaderException(message);
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (RemoteCacheStoreConfig) config;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      StreamingMarshaller marshaller = getMarshaller();

      if (marshaller == null) {throw new IllegalStateException("Null marshaller not allowed!");}
      remoteCacheManager = new RemoteCacheManager(marshaller, config.getHotRodClientProperties());
      if (config.getRemoteCacheName().equals(CacheContainer.DEFAULT_CACHE_NAME))
         remoteCache = remoteCacheManager.getCache();
      else
         remoteCache = remoteCacheManager.getCache(config.getRemoteCacheName());
   }

   @Override
   public void stop() throws CacheLoaderException {
      remoteCacheManager.stop();
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return RemoteCacheStoreConfig.class;
   }

   private long toSeconds(long millis, InternalCacheEntry entry, String desc) {
      if (millis > 0 && millis < 1000) {
         if (log.isTraceEnabled()) {
            log.trace("Adjusting " + desc + " time for (k,v): (" + entry.getKey() + ", " + entry.getValue() + ") from "
                  + millis + " millis to 1 sec, as milliseconds are not supported by HotRod");
         }
         return 1;
      }
      return TimeUnit.MILLISECONDS.toSeconds(millis);
   }

   private Set<InternalCacheEntry> convertToInternalCacheEntries(Map<Object, Object> map) {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>(map.size());
      Set<Map.Entry<Object, Object>> set = map.entrySet();
      for (Map.Entry<Object, Object> e : set) {
         result.add((InternalCacheEntry) e.getValue());
      }
      return result;
   }
}
