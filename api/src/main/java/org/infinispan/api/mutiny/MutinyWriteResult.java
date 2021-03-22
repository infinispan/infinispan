package org.infinispan.api.mutiny;

import org.infinispan.api.Experimental;

/**
 * Write result for write operations on the Cache
 *
 * @since 14.0
 */
@Experimental
public class MutinyWriteResult<K> {
   private final K key;
   private final Throwable throwable;

   public MutinyWriteResult(K key, Throwable exception) {
      this.key = key;
      this.throwable = exception;
   }

   public MutinyWriteResult(K key) {
      this.key = key;
      this.throwable = null;
   }

   public K getKey() {
      return key;
   }

   public boolean isError() {
      return throwable != null;
   }
}
