package org.infinispan.jcache;

import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerRegistration;

/**
 * The reference implementation of the {@link CacheEntryListenerRegistration}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Galder Zamarreño
 */
public class JCacheListenerRegistration<K, V> implements CacheEntryListenerRegistration<K, V> {

   private CacheEntryListener<? super K, ? super V> listener;
   private CacheEntryEventFilter<? super K, ? super V> filter;
   private boolean isOldValueRequired;
   private boolean isSynchronous;

   /**
    * Constructs an {@link JCacheListenerRegistration}.
    *
    * @param listener           the {@link CacheEntryListener}
    * @param filter             the optional {@link CacheEntryEventFilter}
    * @param isOldValueRequired if the old value is required for events with
    *                           this listener
    * @param isSynchronous      if the listener should block the thread
    *                           causing the event
    */
   public JCacheListenerRegistration(
         CacheEntryListener<? super K, ? super V> listener,
         CacheEntryEventFilter<? super K, ? super V> filter,
         boolean isOldValueRequired,
         boolean isSynchronous) {
      this.listener = listener;
      this.filter = filter;
      this.isOldValueRequired = isOldValueRequired;
      this.isSynchronous = isSynchronous;
   }

   @Override
   public CacheEntryEventFilter<? super K, ? super V> getCacheEntryFilter() {
      return filter;
   }

   @Override
   public CacheEntryListener<? super K, ? super V> getCacheEntryListener() {
      return listener;
   }

   @Override
   public boolean isOldValueRequired() {
      return isOldValueRequired;
   }

   @Override
   public boolean isSynchronous() {
      return isSynchronous;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JCacheListenerRegistration that = (JCacheListenerRegistration) o;

      if (isOldValueRequired != that.isOldValueRequired) return false;
      if (isSynchronous != that.isSynchronous) return false;
      if (filter != null ? !filter.equals(that.filter) : that.filter != null)
         return false;
      if (listener != null ? !listener.equals(that.listener) : that.listener != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = listener != null ? listener.hashCode() : 0;
      result = 31 * result + (filter != null ? filter.hashCode() : 0);
      result = 31 * result + (isOldValueRequired ? 1 : 0);
      result = 31 * result + (isSynchronous ? 1 : 0);
      return result;
   }

}
