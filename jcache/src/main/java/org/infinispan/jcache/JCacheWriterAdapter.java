package org.infinispan.jcache;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.spi.AbstractCacheStore;

import javax.cache.integration.CacheWriter;

public class JCacheWriterAdapter<K, V> extends AbstractCacheStore {

   private CacheWriter<? super K, ? super V> delegate;

   public JCacheWriterAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
   }

   public void setCacheWriter(CacheWriter<? super K, ? super V> delegate) {
      this.delegate = delegate;
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      delegate.write(new JCacheEntry(entry.getKey(), entry.getValue()));
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      // TODO
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      // TODO
   }

   @Override
   public void clear() throws CacheLoaderException {
      // TODO
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      delegate.delete(key);
      return false;
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      //TODO
      return null;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // TODO
   }
}
