package org.infinispan.loaders.remote;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Cache store that delegates the call to a infinispan cluster. Communication between this cache store and the remote
 * cluster is achieved through the java HotRod client: this assures fault tolerance and smart dispatching of calls to
 * the nodes that have the highest chance of containing the given key.
 * <p/>
 * Due to certain HotRod constraints, this cache store does not support preload and also cannot be used for provide
 * state. Setting <b>fetchPersistentState</b> is not allowed.
 * <p/>
 * Purging elements is also not possible, as HotRod does not support the fetching of all remote keys (this would be a
 * very costly operation as well). Purging takes place at the remote end (infinispan cluster).
 * <p/>
 *
 * @see org.infinispan.loaders.remote.RemoteCacheStoreConfig
 * @see <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
@CacheLoaderMetadata(configurationClass = RemoteCacheStoreConfig.class)
public class RemoteCacheStore extends AbstractCacheStore {

   private static Log log = LogFactory.getLog(RemoteCacheStore.class);

   private volatile RemoteCacheStoreConfig config;
   private volatile RemoteCacheManager remoteCacheManager;
   private volatile Cache<Object, InternalCacheEntry> remoteCache;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return remoteCache.get(key);
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.trace("Skipping purge call, as this is performed on the remote cache.");
      }
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      if (log.isTraceEnabled()) {
         log.trace("Adding entry: " + entry);
      }
      remoteCache.put(entry.getKey(), entry, toSeconds(entry.getLifespan(), entry, LIFESPAN), TimeUnit.SECONDS, toSeconds(entry.getMaxIdle(), entry, MAXIDLE), TimeUnit.SECONDS);
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

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      fail();
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      fail();
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
      fail();
      return null;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      fail();
      return null;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      fail();
      return null;
   }

   private void fail() throws CacheLoaderException {
      String message = "RemoteCacheStore can only run in shared mode and it doesn't support preload!";
      log.error(message);
      throw new CacheLoaderException(message);
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, Marshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (RemoteCacheStoreConfig) config;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      Marshaller marshaller = getMarshaller();

      if (marshaller == null) {throw new IllegalStateException("Null marshaller not allowed!");}
      remoteCacheManager = new RemoteCacheManager(new InternalCacheEntryMarshaller(marshaller), config.getHotRodClientProperties());
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
}
