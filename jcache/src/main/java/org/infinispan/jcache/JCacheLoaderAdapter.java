package org.infinispan.jcache;

import org.infinispan.jcache.logging.Log;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.util.logging.LogFactory;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoader;

public class JCacheLoaderAdapter<K, V> implements org.infinispan.persistence.spi.CacheLoader {

   private static final Log log = LogFactory.getLog(JCacheLoaderAdapter.class, Log.class);

   private CacheLoader<K, V> delegate;
   private InitializationContext ctx;

   public JCacheLoaderAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
   }

   public void setCacheLoader(CacheLoader<K, V> delegate) {
      this.delegate = delegate;
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
   }

   @SuppressWarnings("unchecked")
   @Override
   public MarshalledEntry load(Object key) throws CacheLoaderException {
      Entry<K, V> e = delegate.load((K) key);
      // TODO or whatever type of entry is more appropriate?
      return e == null ? null : ctx.getMarshalledEntryFactory().newMarshalledEntry(e.getValue(), e.getValue(), null);
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
   public boolean contains(Object key) {
      return load(key) != null;
   }
}
