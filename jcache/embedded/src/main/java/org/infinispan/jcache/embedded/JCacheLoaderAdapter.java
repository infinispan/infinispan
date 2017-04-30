package org.infinispan.jcache.embedded;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.logging.Log;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;

public class JCacheLoaderAdapter<K, V> implements org.infinispan.persistence.spi.CacheLoader {

   private static final Log log = LogFactory.getLog(JCacheLoaderAdapter.class, Log.class);

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
   public MarshalledEntry load(Object key) throws PersistenceException {
      V value = loadValue(key);

      if (value != null) {
         Duration expiry = Expiration.getExpiry(expiryPolicy, Expiration.Operation.CREATION);
         long now = ctx.getTimeService().wallClockTime(); // ms
         if (expiry == null || expiry.isEternal()) {
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, value, null);
         } else {
            long exp = now + expiry.getTimeUnit().toMillis(expiry.getDurationAmount());
            JCacheInternalMetadata meta = new JCacheInternalMetadata(now, exp);
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, value, meta);
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
      return load(key) != null;
   }

}
