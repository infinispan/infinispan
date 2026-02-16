package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.string.XXH3;
import org.infinispan.server.resp.response.SetResponse;

public class SetOperation {

   public static final byte[] GET_BYTES = "GET".getBytes(StandardCharsets.US_ASCII);
   public static final byte[] NX_BYTES = "NX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] XX_BYTES = "XX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] KEEP_TTL_BYTES = "KEEPTTL".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFEQ_BYTES = "IFEQ".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFNE_BYTES = "IFNE".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFDEQ_BYTES = "IFDEQ".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFDNE_BYTES = "IFDNE".getBytes(StandardCharsets.US_ASCII);
   private static final CompletionStage<SetResponse> MISSING_ARGUMENTS = CompletableFuture.failedFuture(new IllegalStateException("Missing arguments"));

   public static CompletionStage<SetResponse> performOperation(AdvancedCache<byte[], byte[]> cache, List<byte[]> arguments, TimeService timeService, String command) {
      try {
         if (arguments.size() < 2) return MISSING_ARGUMENTS;

         SetOperationOptions options = new SetOperationOptions(arguments, timeService, command);
         if (options.operationType == XX_BYTES) {
            return performOperationWithXX(cache, options, timeService);
         }

         // Handle conditional operations: IFEQ, IFNE, IFDEQ, IFDNE
         if (options.hasConditionalMatch()) {
            return performOperationWithCondition(cache, options, timeService);
         }

         CompletionStage<byte[]> cacheOperation;
         if (options.isKeepingTtl()) {
            cacheOperation =  cache.getCacheEntryAsync(options.key)
                  .thenCompose(e -> performOperation(cache, options, e != null ? extractCurrentTTL(e, timeService) : -1));
         } else if (options.isReturningPrevious()) {
            cacheOperation =  cache.getAsync(options.key)
                  .thenCompose(
                     e -> performOperation(cache, options, options.expirationMs)
                     );
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

               long exp = options.expirationMs;
               if (options.isKeepingTtl()) {
                  exp = extractCurrentTTL(e, timeService);
               }

               byte[] prev = e.getValue();
               return cache.replaceAsync(key, prev, value, exp, TimeUnit.MILLISECONDS)
                     .thenApply(b -> new SetResponse(prev, options.isReturningPrevious(), b));
            });
   }

   private static CompletionStage<SetResponse> performOperationWithCondition(AdvancedCache<byte[], byte[]> cache, SetOperationOptions options, TimeService timeService) {
      byte[] key = options.key;
      byte[] value = options.value;

      return cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               byte[] currentValue = (e != null && !e.isNull()) ? e.getValue() : null;
               boolean shouldSet = evaluateCondition(options, currentValue);

               if (!shouldSet) {
                  // Condition not met - return the previous value if GET was specified
                  return CompletableFuture.completedFuture(new SetResponse(currentValue, options.isReturningPrevious(), false));
               }

               long exp = options.expirationMs;
               if (options.isKeepingTtl() && e != null && !e.isNull()) {
                  exp = extractCurrentTTL(e, timeService);
               }

               if (currentValue == null) {
                  // Key doesn't exist - create it
                  return cache.putAsync(key, value, exp, TimeUnit.MILLISECONDS)
                        .thenApply(prev -> new SetResponse(prev, options.isReturningPrevious(), true));
               } else {
                  // Key exists - replace it
                  return cache.replaceAsync(key, currentValue, value, exp, TimeUnit.MILLISECONDS)
                        .thenApply(b -> new SetResponse(currentValue, options.isReturningPrevious(), b));
               }
            });
   }

   private static boolean evaluateCondition(SetOperationOptions options, byte[] currentValue) {
      ConditionType conditionType = options.conditionType;
      byte[] conditionValue = options.conditionValue;

      switch (conditionType) {
         case IFEQ:
            // Set only if current value equals conditionValue; don't create if missing
            if (currentValue == null) return false;
            return Arrays.equals(currentValue, conditionValue);
         case IFNE:
            // Set only if current value doesn't equal conditionValue; create if missing
            if (currentValue == null) return true;
            return !Arrays.equals(currentValue, conditionValue);
         case IFDEQ:
            // Set only if current value's digest equals conditionValue; don't create if missing
            if (currentValue == null) return false;
            long currentDigest = XXH3.hash64(currentValue);
            long targetDigest = XXH3.parseHexDigest(new String(conditionValue, StandardCharsets.US_ASCII));
            return currentDigest == targetDigest;
         case IFDNE:
            // Set only if current value's digest doesn't equal conditionValue; create if missing
            if (currentValue == null) return true;
            long currentDigest2 = XXH3.hash64(currentValue);
            long targetDigest2 = XXH3.parseHexDigest(new String(conditionValue, StandardCharsets.US_ASCII));
            return currentDigest2 != targetDigest2;
         default:
            return true;
      }
   }

   enum ConditionType {
      NONE, IFEQ, IFNE, IFDEQ, IFDNE
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
      private ConditionType conditionType;
      private byte[] conditionValue;

      public SetOperationOptions(List<byte[]> arguments, TimeService timeService, String command) {
         this.arguments = arguments;
         this.key = null;
         this.value = null;
         this.expirationMs = -1;
         this.keepTtl = false;
         this.setAndReturnPrevious = false;
         this.operationType = null;
         this.conditionType = ConditionType.NONE;
         this.conditionValue = null;
         parseAndLoadOptions(timeService, command);
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

      public boolean hasConditionalMatch() {
         return conditionType != ConditionType.NONE;
      }

      public void withCondition(ConditionType type, byte[] value) {
         this.conditionType = type;
         this.conditionValue = value;
      }

      private void parseAndLoadOptions(TimeService timeService, String command) {
         withKey(arguments.get(0));
         withValue(arguments.get(1));

         // Bellow here we parse the optional arguments for the SET command:
         //
         // * `GET`: return the previous value with this key or nil;
         // * `NX` or `XX`: putIfAbsent or putIfPresent. Returns nil if failed, if `GET` is present, it takes precedence.
         // * `IFEQ`, `IFNE`, `IFDEQ`, `IFDNE`: conditional set based on value or digest comparison.
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
                     if (!RespUtil.caseInsensitiveAsciiCheck('X', arg[1])) break;
                     if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
                     withOperationType(NX_BYTES);
                     continue;
                  case 'X':
                  case 'x':
                     if (!RespUtil.caseInsensitiveAsciiCheck('X', arg[1])) break;
                     if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
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

                     long time = ArgumentUtils.toLong(arguments.get(i + 1));
                     if (time <= 0) throw new IllegalArgumentException(String.format("invalid expire time in '%s' command", command));

                     withExpiration(expiration.convert(time, timeService));
                     i++;
                     continue;
                  case 'I':
                  case 'i':
                     // IFEQ or IFNE (4 chars)
                     if (RespUtil.isAsciiBytesEquals(IFEQ_BYTES, arg)) {
                        if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
                        if (i + 1 >= arguments.size()) throw new IllegalArgumentException("IFEQ requires a value argument");
                        withCondition(ConditionType.IFEQ, arguments.get(i + 1));
                        i++;
                        continue;
                     } else if (RespUtil.isAsciiBytesEquals(IFNE_BYTES, arg)) {
                        if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
                        if (i + 1 >= arguments.size()) throw new IllegalArgumentException("IFNE requires a value argument");
                        withCondition(ConditionType.IFNE, arguments.get(i + 1));
                        i++;
                        continue;
                     }
                     break;
               }

               throw new IllegalArgumentException("Unknown argument for SET operation");
            }

            // IFDEQ or IFDNE (5 chars)
            if (arg.length == 5) {
               if (RespUtil.isAsciiBytesEquals(IFDEQ_BYTES, arg)) {
                  if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
                  if (i + 1 >= arguments.size()) throw new IllegalArgumentException("IFDEQ requires a digest argument");
                  withCondition(ConditionType.IFDEQ, arguments.get(i + 1));
                  i++;
                  continue;
               } else if (RespUtil.isAsciiBytesEquals(IFDNE_BYTES, arg)) {
                  if (operationType != null || hasConditionalMatch()) throw new IllegalArgumentException("NX, XX, IFEQ, IFNE, IFDEQ, IFDNE options are mutually exclusive");
                  if (i + 1 >= arguments.size()) throw new IllegalArgumentException("IFDNE requires a digest argument");
                  withCondition(ConditionType.IFDNE, arguments.get(i + 1));
                  i++;
                  continue;
               }
               throw new IllegalArgumentException("Unknown argument for SET operation");
            }

            // `GET` argument.
            if (arg.length == 3 && RespUtil.isAsciiBytesEquals(GET_BYTES, arg)) {
               withReturnPrevious();
               continue;
            }

            // `KEEPTTL` argument.
            if (arg.length == 7 && RespUtil.isAsciiBytesEquals(KEEP_TTL_BYTES, arg)) {
               if (isUsingExpiration()) throw new IllegalArgumentException("KEEPTTL and EX/PX/EXAT/PXAT are mutually exclusive");
               withKeepTtl();
               continue;
            }

            throw new IllegalArgumentException("Unknown argument for SET operation");
         }
      }
   }
}
