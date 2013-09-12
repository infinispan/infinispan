package org.infinispan.jcache;

import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;

import javax.cache.integration.CacheWriter;

public class JCacheWriterAdapter<K, V> implements org.infinispan.persistence.spi.CacheWriter {

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
   public void init(InitializationContext ctx) {
   }

   @Override
   public void write(MarshalledEntry entry) {
      delegate.write(new JCacheEntry(entry.getKey(), entry.getValue()));
   }

   @Override
   public boolean delete(Object key) {
      delegate.delete(key);
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }
}
