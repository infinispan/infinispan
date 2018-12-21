package org.infinispan.jcache.embedded;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;

import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.Expiration;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;

public class JCacheLoaderAdapter<K, V> implements org.infinispan.persistence.spi.CacheLoader {

   private CacheLoader<K, V> delegate;
   private InitializationContext ctx;
   private ExpiryPolicy expiryPolicy;

   public JCacheLoaderAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
   }

   public void setCacheLoader(CacheLoader<K, V> delegate) {
      this.delegate = delegate;
   }

   public void setExpiryPolicy(ExpiryPolicy expiryPolicy) {
      this.expiryPolicy = expiryPolicy;
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
   }

   @Override
   public MarshallableEntry<K,V> loadEntry(Object key) throws PersistenceException {
      V value = loadValue(key);
      if (value != null) {
         Duration expiry = Expiration.getExpiry(expiryPolicy, Expiration.Operation.CREATION);
         if (expiry == null || expiry.isEternal()) {
            return ctx.<K,V>getMarshallableEntryFactory().create(key, value);
         } else {
            long now = ctx.getTimeService().wallClockTime();
            long exp = now + expiry.getTimeUnit().toMillis(expiry.getDurationAmount());
            Metadata meta = new EmbeddedMetadata.Builder().lifespan(exp - now).build();
            return ctx.<K,V>getMarshallableEntryFactory().create(key, value, meta, now, -1);
         }
      }
      return null;
   }

   @SuppressWarnings("unchecked")
   private V loadValue(Object key) {
      try {
         return delegate.load((K) key);
      } catch (Exception e) {
         throw Exceptions.launderCacheLoaderException(e);
      }
   }

   @Override
   public void start() throws PersistenceException {
      // No-op
   }

   @Override
   public void stop() throws PersistenceException {
      // No-op
   }

   @Override
   public boolean contains(Object key) {
      return loadEntry(key) != null;
   }

}
