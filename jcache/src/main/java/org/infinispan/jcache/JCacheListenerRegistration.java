/**
 *  Copyright 2011 Terracotta, Inc.
 *  Copyright 2011 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
