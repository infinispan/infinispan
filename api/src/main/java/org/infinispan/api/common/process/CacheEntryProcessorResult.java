package org.infinispan.api.common.process;

import org.infinispan.api.Experimental;

/**
 * Write result for process operations on the Cache
 *
 * @since 14.0
 */
@Experimental
public interface CacheEntryProcessorResult<K, T> {
   K key();

   T result();

   Throwable error();

   static <K, T> CacheEntryProcessorResult<K, T> onResult(K key, T result) {
      return new Impl<>(key, result, null);
   }

   static <K, T> CacheEntryProcessorResult<K, T> onError(K key, Throwable throwable) {
      return new Impl<>(key, null, throwable);
   }

   class Impl<K, T> implements CacheEntryProcessorResult<K, T> {
      private final K key;
      private final T result;
      private final Throwable throwable;

      public Impl(K key, T result, Throwable throwable) {
         this.key = key;
         this.result = result;
         this.throwable = throwable;
      }

      @Override
      public K key() {
         return key;
      }

      @Override
      public T result() {
         return result;
      }

      @Override
      public Throwable error() {
         return throwable;
      }
   }
}
