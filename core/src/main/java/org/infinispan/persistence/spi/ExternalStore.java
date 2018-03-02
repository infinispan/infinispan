package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;

/**
 * Basic interface for interacting with an external store in a read-write mode.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface ExternalStore<K, V> extends CacheLoader<K, V>, CacheWriter<K, V> {
   @Override
   default boolean isAvailable() {
      return CacheWriter.super.isAvailable();
   }

   /**
    * Method to be used to destroy and clean up any resources associated with this store. This is normally only
    * useful for non shared stores.
    * <p>
    * This method will ensure the store is stopped and properly cleans up all resources for it.
    * @implSpec Default implementation just invokes {@link #stop()}
    */
   default void destroy() { stop(); }
}
