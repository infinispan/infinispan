package org.infinispan.loaders.decorators;

import org.infinispan.Cache;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.modifications.Modification;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * Simple delegate that delegates all calls.  This is intended as a building block for other decorators who wish to add
 * behavior to certain calls only.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AbstractDelegatingStore implements CacheStore {

   CacheStore delegate;

   public AbstractDelegatingStore(CacheStore delegate) {
      this.delegate = delegate;
   }

   public void setDelegate(CacheStore delegate) {
      this.delegate = delegate;
   }

   public CacheStore getDelegate() {
      return delegate;
   }

   public static CacheLoader undelegateCacheLoader(CacheLoader store) {
      return store instanceof AbstractDelegatingStore ? ((AbstractDelegatingStore)store).getDelegate() : store;
   }

   @Override
   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      delegate.removeAll(keys);
   }

   @Override
   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      delegate.store(ed);
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
   public void purgeExpired() throws CacheLoaderException {
      delegate.purgeExpired();
   }

   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      delegate.commit(tx);
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      delegate.rollback(tx);
   }

   @Override
   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      delegate.prepare(list, tx, isOnePhase);
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      delegate.init(config, cache, m);
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

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return delegate.containsKey(key);
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return delegate.getConfigurationClass();
   }

   @Override
   public void start() throws CacheLoaderException {
      delegate.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      delegate.stop();
   }

   @Override
   public CacheStoreConfig getCacheStoreConfig() {
      return delegate.getCacheStoreConfig();
   }
}
