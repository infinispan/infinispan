package org.infinispan.server.resp.commands.string;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.commands.ArgumentUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class CounterIncOrDec {
   private CounterIncOrDec() {

   }
   static CompletionStage<Long> counterIncOrDec(Cache<byte[], byte[]> cache, byte[] key, boolean increment) {
      return counterIncOrDecBy(cache, key, increment ? 1 : -1);
   }

   static CompletionStage<Long> counterIncOrDecBy(Cache<byte[], byte[]> cache, byte[] key, long by) {
      return cache.getAsync(key)
            .thenCompose(currentValueBytes -> {
               if (currentValueBytes != null) {
                  long prevIntValue;
                  try {
                     prevIntValue = ArgumentUtils.toLong(currentValueBytes) + by;
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not an integer or out of range");
                  }
                  byte[] newValueBytes = ArgumentUtils.toByteArray(prevIntValue);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevIntValue);
                           }
                           return counterIncOrDecBy(cache, key, by);
                        });
               }
               byte[] valueToPut = ArgumentUtils.toByteArray(by);
               return cache.putIfAbsentAsync(key, valueToPut)
                     .thenCompose(prev -> {
                        if (prev != null) {
                           return counterIncOrDecBy(cache, key, by);
                        }
                        return CompletableFuture.completedFuture(by);
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
                  double prevDoubleValue;
                  try {
                     prevDoubleValue = ArgumentUtils.toDouble(currentValueBytes) + by;
                  } catch (NumberFormatException e) {
                     throw new CacheException("value is not a valid float");
                  }
                  byte[] newValueBytes = ArgumentUtils.toByteArray(prevDoubleValue);
                  return cache.replaceAsync(key, currentValueBytes, newValueBytes)
                        .thenCompose(replaced -> {
                           if (replaced) {
                              return CompletableFuture.completedFuture(prevDoubleValue);
                           }
                           return counterIncByDouble(cache, key, by);
                        });
               }
               byte[] valueToPut = ArgumentUtils.toByteArray(by);
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
