package org.infinispan.server.resp.commands.string;

import io.netty.util.CharsetUtil;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class CounterIncOrDec {
   private CounterIncOrDec() {

   }
   static CompletionStage<Long> counterIncOrDec(Cache<byte[], byte[]> cache, byte[] key, boolean increment) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  // Numbers are always ASCII
                  String prevValue = new String(currentValueBytes, CharsetUtil.US_ASCII);
                  long prevIntValue;
                  try {
                     prevIntValue = Long.parseLong(prevValue) + (increment ? 1 : -1);
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not an integer or out of range");
                  }
                  String newValueString = String.valueOf(prevIntValue);
                  byte[] newValueBytes = newValueString.getBytes(CharsetUtil.US_ASCII);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevIntValue);
                           }
                           return counterIncOrDec(cache, key, increment);
                        });
               }
               long longValue = increment ? 1 : -1;
               byte[] valueToPut = String.valueOf(longValue).getBytes(CharsetUtil.US_ASCII);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncOrDec(cache, key, increment);
                        }
                        return CompletableFuture.completedFuture(longValue);
                     });
            });
   }
}
