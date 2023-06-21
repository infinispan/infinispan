package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.response.SetResponse;
import org.infinispan.util.concurrent.CompletionStages;

public class SetOperation {

   private static final byte[] GET_BYTES = "GET".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] NX_BYTES = "NX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] XX_BYTES = "XX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] KEEP_TTL_BYTES = "KEEPTTL".getBytes(StandardCharsets.US_ASCII);
   private static final CompletionStage<SetResponse> MISSING_ARGUMENTS = CompletableFuture.failedFuture(new IllegalStateException("Missing arguments"));

   public static CompletionStage<SetResponse> performOperation(AdvancedCache<byte[], byte[]> cache, List<byte[]> arguments, TimeService timeService) {
      try {
         if (arguments.size() < 2) return MISSING_ARGUMENTS;

         SetOperationOptions options = new SetOperationOptions(arguments, timeService);
         if (options.operationType == XX_BYTES) {
            return performOperationWithXX(cache, options, timeService);
         }

         CompletionStage<byte[]> cacheOperation;
         if (options.isKeepingTtl()) {
            cacheOperation =  cache.getCacheEntryAsync(options.key)
                  .thenCompose(e -> performOperation(cache, options, e != null ? extractCurrentTTL(e, timeService) : -1));
         } else {
            cacheOperation = performOperation(cache, options, options.expirationMs);
         }

         if (CompletionStages.isCompletedSuccessfully(cacheOperation)) {
            return CompletableFuture.completedFuture(parseResponse(options, CompletionStages.join(cacheOperation)));
         }

         return cacheOperation.thenApply(v -> parseResponse(options, v));
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
   }

   private static CompletionStage<byte[]> performOperation(AdvancedCache<byte[], byte[]> cache, SetOperationOptions options, long expiration) {
      byte[] key = options.key;
      byte[] value = options.value;
      return options.operationType == null
            ? cache.putAsync(key, value, expiration, TimeUnit.MILLISECONDS)
            : cache.putIfAbsentAsync(key, value, expiration, TimeUnit.MILLISECONDS);
   }

   private static SetResponse parseResponse(SetOperationOptions options, byte[] v) {
      return options.operationType == null
            ? new SetResponse(v, options.isReturningPrevious())
            : new SetResponse(v, options.isReturningPrevious(), v == null);
   }

   private static CompletionStage<SetResponse> performOperationWithXX(AdvancedCache<byte[], byte[]> cache, SetOperationOptions options, TimeService timeService) {
      byte[] key = options.key;
      byte[] value = options.value;
      return cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               if (e == null || e.isNull()) {
                  return CompletableFuture.completedFuture(new SetResponse(null, options.isReturningPrevious(), false));
               }

               long exp = -1;
               if (options.isKeepingTtl()) {
                  exp = extractCurrentTTL(e, timeService);
               }

               byte[] prev = e.getValue();
               return cache.replaceAsync(key, prev, value, exp, TimeUnit.MILLISECONDS)
                     .thenApply(b -> new SetResponse(prev, options.isReturningPrevious(), b));
            });
   }

   private static long extractCurrentTTL(CacheEntry<?, ?> entry, TimeService timeService) {
      long lifespan = entry.getLifespan();
      long delta = timeService.instant().toEpochMilli() - entry.getCreated();
      return lifespan - delta;
   }

   private static class SetOperationOptions {
      private final List<byte[]> arguments;
      private byte[] key;
      private byte[] value;
      private long expirationMs;
      private boolean keepTtl;
      private boolean setAndReturnPrevious;
      private byte[] operationType;

      public SetOperationOptions(List<byte[]> arguments, TimeService timeService) {
         this.arguments = arguments;
         this.key = null;
         this.value = null;
         this.expirationMs = -1;
         this.keepTtl = false;
         this.setAndReturnPrevious = false;
         this.operationType = null;
         parseAndLoadOptions(timeService);
      }

      public void withKey(byte[] key) {
         this.key = key;
      }

      public void withValue(byte[] value) {
         this.value = value;
      }

      public void withReturnPrevious() {
         this.setAndReturnPrevious = true;
      }

      public void withExpiration(long expirationMs) {
         this.expirationMs = expirationMs;
      }

      public void withOperationType(byte[] operationType) {
         this.operationType = operationType;
      }

      public void withKeepTtl() {
         this.keepTtl = true;
      }

      public boolean isKeepingTtl() {
         return this.keepTtl;
      }

      public boolean isReturningPrevious() {
         return this.setAndReturnPrevious;
      }

      public boolean isUsingExpiration() {
         return expirationMs > 0 || isKeepingTtl();
      }

      private void parseAndLoadOptions(TimeService timeService) {
         withKey(arguments.get(0));
         withValue(arguments.get(1));

         // Bellow here we parse the optional arguments for the SET command:
         //
         // * `GET`: return the previous value with this key or nil;
         // * `NX` or `XX`: putIfAbsent or putIfPresent. Returns nil if failed, if `GET` is present, it takes precedence.
         //
         // And expiration related parameters:
         //
         // `EX` seconds: TTL in seconds;
         // `PX` milliseconds: TTL in ms;
         // `EXAT` timestamp: Unix time for key expiration, seconds;
         // `PXAT` timestamp: Unix time for key expiration, milliseconds;
         // `KEEPTTL`: keep the key current TTL.
         //
         // Each of the time arguments are exclusive, only one is present at a time.
         // All these arguments can be in any order. Expiration must be followed by the proper value.
         for (int i = 2; i < arguments.size(); i++) {
            byte[] arg = arguments.get(i);

            // `NX`, `XX` or expiration.
            if (arg.length == 2 || arg.length == 4) {
               switch (arg[0]) {
                  case 'N':
                  case 'n':
                     if (!Util.caseInsensitiveAsciiCheck('X', arg[1])) break;
                     if (operationType != null) throw new IllegalArgumentException("NX and XX options are mutually exclusive");
                     withOperationType(NX_BYTES);
                     continue;
                  case 'X':
                  case 'x':
                     if (!Util.caseInsensitiveAsciiCheck('X', arg[1])) break;
                     if (operationType != null) throw new IllegalArgumentException("NX and XX options are mutually exclusive");
                     withOperationType(XX_BYTES);
                     continue;
                  case 'E':
                  case 'P':
                  case 'e':
                  case 'p':
                     // Throws an exception if invalid.
                     RespExpiration expiration = RespExpiration.valueOf(arg);
                     if (isUsingExpiration()) throw new IllegalArgumentException("Only one expiration option should be used on SET");
                     if (isKeepingTtl()) throw new IllegalArgumentException("KEEPTTL and EX/PX/EXAT/PXAT are mutually exclusive");
                     if (i + 1 > arguments.size()) throw new IllegalArgumentException("No argument accompanying expiration");

                     withExpiration(expiration.convert(Long.parseLong(new String(arguments.get(i + 1), StandardCharsets.US_ASCII)), timeService));
                     i++;
                     continue;
               }

               throw new IllegalArgumentException("Unknown argument for SET operation");
            }

            // `GET` argument.
            if (arg.length == 3 && Util.isAsciiBytesEquals(GET_BYTES, arg)) {
               withReturnPrevious();
               continue;
            }

            // `KEEPTTL` argument.
            if (arg.length == 7 && Util.isAsciiBytesEquals(KEEP_TTL_BYTES, arg)) {
               if (isUsingExpiration()) throw new IllegalArgumentException("KEEPTTL and EX/PX/EXAT/PXAT are mutually exclusive");
               withKeepTtl();
               continue;
            }

            throw new IllegalArgumentException("Unknown argument for SET operation");
         }
      }
   }
}
