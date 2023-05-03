package org.infinispan.server.resp.commands.string;

import org.infinispan.Cache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * StringMutators
 *
 * Container class for methods that change string entries on the cache
 */
final class StringMutators {
   private StringMutators() {

   }
   static CompletionStage<Long> append(Cache<byte[], byte[]> cache, byte[] key, byte[] appendix) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  byte[] newValueBytes = new byte[currentValueBytes.length + appendix.length];
                  System.arraycopy(currentValueBytes, 0, newValueBytes, 0, currentValueBytes.length);
                  System.arraycopy(appendix, 0, newValueBytes, currentValueBytes.length, appendix.length);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture((long)newValueBytes.length);
                           }
                           return append(cache, key, appendix);
                        });
               }
               return cache.putIfAbsentAsync(key, appendix)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return append(cache, key, appendix);
                        }
                        return CompletableFuture.completedFuture((long)appendix.length);
                     });
            });
   }
}
