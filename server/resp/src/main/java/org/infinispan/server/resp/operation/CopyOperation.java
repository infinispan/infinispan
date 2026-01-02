package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.RespUtil;

public class CopyOperation {
   public static final byte[] REPLACE_BYTES = "REPLACE".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] DB_BYTES = "DB".getBytes(StandardCharsets.US_ASCII);

   public static CompletionStage<Integer> perform(Cache<byte[], byte[]> originalCache, EmbeddedCacheManager cacheManager, List<byte[]> arguments) {
      boolean isReplace = false;
      Cache<byte[], byte[]> cacheToCopyTo = originalCache;
      if (arguments.size() > 2) {
         for (int i = 2; i < arguments.size(); i++) {
            byte[] arg = arguments.get(i);

            if (RespUtil.isAsciiBytesEquals(DB_BYTES, arg)) {
               if (i + 1 == arguments.size()) throw new IllegalArgumentException("No database name provided.");

               String dbName = new String(arguments.get(++i), StandardCharsets.US_ASCII);
               cacheToCopyTo = cacheManager.getCache(dbName);
            } else if(RespUtil.isAsciiBytesEquals(REPLACE_BYTES, arg)) {
               isReplace = true;
            } else {
               throw new IllegalArgumentException("Unknown argument for COPY operation");
            }
         }
      }

      return copy(originalCache, cacheToCopyTo, arguments, isReplace);
   }

   private static CompletionStage<Integer> copy(Cache<byte[], byte[]> originalCache, Cache<byte[], byte[]> cacheToCopyTo,
                                                List<byte[]> arguments, boolean isReplace) {
      byte[] copiableKeyBytes = arguments.get(0);
      byte[] newKeyBytes = arguments.get(1);

      return originalCache.getAsync(copiableKeyBytes)
            .thenCompose(value -> {
                     if(value != null) {
                        if (isReplace) {
                           return cacheToCopyTo.putAsync(newKeyBytes, value)
                                 .thenCompose(prev -> {
                                    return CompletableFuture.completedFuture(1);
                                 });
                        } else {
                           return cacheToCopyTo.putIfAbsentAsync(newKeyBytes, value)
                                 .thenCompose(prev -> {
                                    if (prev != null) {
                                       return CompletableFuture.completedFuture(0);
                                    }
                                    return CompletableFuture.completedFuture(1);
                                 });
                        }
                     } else  {
                        return CompletableFuture.completedFuture(0);
                     }
                  }
            );
   }
}
