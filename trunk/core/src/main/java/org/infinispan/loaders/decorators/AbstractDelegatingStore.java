package org.infinispan.loaders.decorators;

import org.infinispan.Cache;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.marshall.Marshaller;

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

   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      delegate.removeAll(keys);
   }

   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      delegate.store(ed);
   }

   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      delegate.fromStream(inputStream);
   }

   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      delegate.toStream(outputStream);
   }

   public void clear() throws CacheLoaderException {
      delegate.clear();
   }

   public boolean remove(Object key) throws CacheLoaderException {
      return delegate.remove(key);
   }

   public void purgeExpired() throws CacheLoaderException {
      delegate.purgeExpired();
   }

   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      delegate.commit(tx);
   }

   public void rollback(GlobalTransaction tx) {
      delegate.rollback(tx);
   }

   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      delegate.prepare(list, tx, isOnePhase);
   }

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) throws CacheLoaderException {
      delegate.init(config, cache, m);
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return delegate.load(key);
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return delegate.loadAll();
   }

   public boolean containsKey(Object key) throws CacheLoaderException {
      return delegate.containsKey(key);
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return delegate.getConfigurationClass();
   }

   public void start() throws CacheLoaderException {
      delegate.start();
   }

   public void stop() throws CacheLoaderException {
      delegate.stop();
   }

   public CacheStoreConfig getCacheStoreConfig() {
      return delegate.getCacheStoreConfig();
   }
}
