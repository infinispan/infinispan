package org.infinispan.jcache.embedded;

import javax.cache.integration.CacheWriter;

import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.InitializationContext;

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
      try {
         if (entry.getValue() != null) {
            // TODO: store metadata
            delegate.write(new JCacheEntry(entry.getKey(), entry.getValue()));
         } else {
            // If this is a tombstone, we don't store metadata anyway. We're forced to this by JCache TCK which
            // expects that a remove will result in cache writer delete.
            delegate.delete(entry.getKey());
         }
      } catch (Exception e) {
         throw Exceptions.launderCacheWriterException(e);
      }
   }

   @Override
   public boolean delete(Object key) {
      try {
         delegate.delete(key);
      } catch (Exception e) {
         throw Exceptions.launderCacheWriterException(e);
      }
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

}
