package org.infinispan.jcache;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoader;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.jcache.logging.Log;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.LogFactory;

public class JCacheLoaderAdapter<K, V> extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(JCacheLoaderAdapter.class, Log.class);

   private CacheLoader<K, V> delegate;

   public JCacheLoaderAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
   }

   public void setCacheLoader(CacheLoader<K, V> delegate) {
      this.delegate = delegate;
   }

   @SuppressWarnings("unchecked")
   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      Entry<K, V> e = delegate.load((K) key);
      // TODO or whatever type of entry is more appropriate?
      return e == null ? null : new ImmortalCacheEntry(e.getKey(), e.getValue());
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public void start() throws CacheLoaderException {
      // No-op
   }

   @Override
   public void stop() throws CacheLoaderException {
      // No-op
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      // TODO: Customise this generated block
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      log.trace("Ignoring store invocation");
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      log.trace("Ignoring writing contents of stream to store");
   }

   @Override
   public void clear() throws CacheLoaderException {
      log.trace("Ignoring clear invocation");
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      log.trace("Ignoring removal of key");
      return false; // no-op
   }

   @Override
   public void purgeExpired() {
      log.trace("Ignoring purge expired invocation");
   }

   @Override
   public void commit(GlobalTransaction tx) {
      log.trace("Ignoring transactional commit call");
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      log.trace("Ignoring transactional rollback call");
   }

   @Override
   public void prepare(List<? extends Modification> list,
         GlobalTransaction tx, boolean isOnePhase) {
      log.trace("Ignoring transactional prepare call");
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // No-op
   }

}
