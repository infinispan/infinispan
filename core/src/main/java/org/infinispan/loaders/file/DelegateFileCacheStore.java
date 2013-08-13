package org.infinispan.loaders.file;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.FileCacheStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.AbstractCacheStore;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * This file delegate cache store has been created to allow switching
 * between file cache store and deprecated bucket-based cache store without
 * having to create a second hierarchy of configuration files for the old
 * deprecated cache store.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public final class DelegateFileCacheStore extends AbstractCacheStore {

   private AbstractCacheStore delegate;

   // TODO (FAO Tristan)
   // Rather than having @ConfigurationFor define the type of cache store to
   // create, why not have a cacheLoaderType() or cacheStoreType() method
   // in CacheLoaderConfiguration, and let implementations decide which
   // type cache loader to instantiate? It also gets around the issue of
   // implementations forgetting to add @ConfigurationFor annotation.

   @Override
   public void init(CacheLoaderConfiguration config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      FileCacheStoreConfiguration storeConfiguration = (FileCacheStoreConfiguration) configuration;
      this.delegate = storeConfiguration.deprecatedBucketFormat()
            ? new BucketFileCacheStore()
            : new FileCacheStore();
      this.delegate.init(config, cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      this.delegate.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      this.delegate.stop();
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // No-op, can't delegate protected method...
   }

   @Override
   public void purgeExpired() throws CacheLoaderException {
      delegate.purgeExpired();
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      delegate.store(entry);
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      delegate.fromStream(inputStream);
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      delegate.toStream(outputStream);
   }

   @Override
   public void clear() throws CacheLoaderException {
      delegate.clear();
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      return delegate.remove(key);
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return delegate.load(key);
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return delegate.loadAll();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return delegate.load(numEntries);
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return delegate.loadAllKeys(keysToExclude);
   }

   AbstractCacheStore getCacheStoreDelegate() {
      return delegate;
   }

}
