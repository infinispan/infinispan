package org.infinispan.configuration.cache;

import java.util.Arrays;

import org.infinispan.util.logging.Log;

/**
 * @since 12.0
 */
public enum IndexStorage {
   FILESYSTEM("filesystem"),
   LOCAL_HEAP("local-heap");

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
