package org.infinispan.jcache;

import org.infinispan.jcache.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.util.logging.LogFactory;

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
      V value = loadKey(key);

      if (value != null) {
         Duration expiry = Expiration.getExpiry(expiryPolicy, Expiration.Operation.CREATION);
         long now = ctx.getTimeService().wallClockTime(); // ms
         if (expiry.isEternal()) {
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(value, value, null);
         } else {
            JCacheInternalMetadata meta = new JCacheInternalMetadata(now,
                  expiry.getTimeUnit().toMillis(expiry.getDurationAmount()));
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(value, value, meta);
         }
      }

      return null;
   }

   @SuppressWarnings("unchecked")
   private V loadKey(Object key) {
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
