package org.infinispan.api.reactive;

import org.infinispan.api.Experimental;

/**
 * Write result for write operations on the KeyValueStore
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
@Experimental
public class WriteResult<K> {
   private final K key;
   private final Throwable throwable;

   public WriteResult(K key, Throwable exception) {
      this.key = key;
      this.throwable = exception;
   }

   public WriteResult(K key) {
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
