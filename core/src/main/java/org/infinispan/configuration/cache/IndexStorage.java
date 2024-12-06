package org.infinispan.configuration.cache;

import java.util.Arrays;

import org.infinispan.util.logging.Log;

/**
 * @since 12.0
 */
public enum IndexStorage {

   /**
    * Local filesystem index storage
    */
   FILESYSTEM("filesystem"),

   /**
    * JVM heap index storage, not persisted between restarts. Only suitable for small datasets with low concurrency
    */
   LOCAL_HEAP("local-heap"),

   /**
    * This value will be replaced when the cache configuration is created.
    * If no persistence stores are configured or all of them are purged at startup, {@link #LOCAL_HEAP} is applied by default.
    * Otherwise, {@link #FILESYSTEM} is applied.
    */
   DEFINE_AT_STARTUP("define-at-startup");

   private final String token;

   IndexStorage(String token) {
      this.token = token;
   }

   public static IndexStorage requireValid(String token, Log logger) {
      return Arrays.stream(IndexStorage.values())
            .filter(i -> i.token.equals(token)).findFirst().orElseThrow(logger::invalidIndexStorage);
   }

   @Override
   public String toString() {
      return token;
   }

}
