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
      return counterIncOrDecBy(cache, key, 1, increment);
   }

   static CompletionStage<Long> counterIncOrDecBy(Cache<byte[], byte[]> cache, byte[] key, long by, boolean increment) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  // Numbers are always ASCII
                  String prevValue = new String(currentValueBytes, CharsetUtil.US_ASCII);
                  long prevIntValue;
                  try {
                     prevIntValue = Long.parseLong(prevValue) + (increment ? by : -by);
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
                           return counterIncOrDecBy(cache, key, by, increment);
                        });
               }
               long longValue = increment ? by : -by;
               byte[] valueToPut = String.valueOf(longValue).getBytes(CharsetUtil.US_ASCII);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncOrDecBy(cache, key, by, increment);
                        }
                        return CompletableFuture.completedFuture(longValue);
                     });
            });
   }

   static CompletionStage<Double> counterIncByDouble(Cache<byte[], byte[]> cache, byte[] key, String by) {
      return counterIncByDouble(cache, key, Double.parseDouble(by));
   }

   static CompletionStage<Double> counterIncByDouble(Cache<byte[], byte[]> cache, byte[] key, Double by) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  // Numbers are always ASCII
                  String prevValue = new String(currentValueBytes, CharsetUtil.US_ASCII);
                  double prevDoubleValue;
                  try {
                     prevDoubleValue = Double.parseDouble(prevValue) + by;
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not a valid float");
                  }
                  String newValueString = String.valueOf(prevDoubleValue);
                  byte[] newValueBytes = newValueString.getBytes(CharsetUtil.US_ASCII);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevDoubleValue);
                           }
                           return counterIncByDouble(cache, key, by);
                        });
               }
               byte[] valueToPut = String.valueOf(by).getBytes(CharsetUtil.US_ASCII);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncByDouble(cache, key, by);
                        }
                        return CompletableFuture.completedFuture(by);
                     });
            });
   }
}
