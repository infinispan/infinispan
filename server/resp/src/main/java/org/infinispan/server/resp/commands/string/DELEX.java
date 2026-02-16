package org.infinispan.server.resp.commands.string;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
 * <p>
 * Conditionally removes the specified key based on value or hash digest comparison.
 * <ul>
 *   <li>IFEQ - Remove the key only if its value equals the specified value</li>
 *   <li>IFNE - Remove the key only if its value does NOT equal the specified value</li>
 *   <li>IFDEQ - Remove the key only if its hash digest equals the specified digest</li>
 *   <li>IFDNE - Remove the key only if its hash digest does NOT equal the specified digest</li>
 * </ul>
 *
 * @see <a href="https://redis.io/commands/delex/">DELEX</a>
 * @since 16.2
 */
public class DELEX extends RespCommand implements Resp3Command {

   private static final byte[] IFEQ = "IFEQ".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFNE = "IFNE".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFDEQ = "IFDEQ".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] IFDNE = "IFDNE".getBytes(StandardCharsets.US_ASCII);

   public DELEX() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);

      // Parse optional condition
      Condition condition = Condition.NONE;
      byte[] conditionValue = null;

      if (arguments.size() >= 3) {
         byte[] condArg = arguments.get(1);
         conditionValue = arguments.get(2);

         if (equalsIgnoreCase(condArg, IFEQ)) {
            condition = Condition.IFEQ;
         } else if (equalsIgnoreCase(condArg, IFNE)) {
            condition = Condition.IFNE;
         } else if (equalsIgnoreCase(condArg, IFDEQ)) {
            condition = Condition.IFDEQ;
         } else if (equalsIgnoreCase(condArg, IFDNE)) {
            condition = Condition.IFDNE;
         } else {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      } else if (arguments.size() == 2) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      final Condition finalCondition = condition;
      final byte[] finalConditionValue = conditionValue;

      if (condition == Condition.NONE) {
         // Unconditional delete
         return handler.stageToReturn(
               handler.cache().removeAsync(key).thenApply(prev -> prev != null ? 1L : 0L),
               ctx,
               ResponseWriter.INTEGER
         );
      } else if (condition == Condition.IFEQ) {
         // Conditional equality delete
         return handler.stageToReturn(
               handler.cache().removeAsync(key, conditionValue).thenApply(removed -> (boolean) removed ? 1L : 0L),
               ctx,
               ResponseWriter.INTEGER
         );
      } else {
         // Conditional delete - need to get the value first
         return handler.stageToReturn(
               handler.cache().getAsync(key).thenCompose(value -> {
                  if (value == null) {
                     // Key doesn't exist
                     return java.util.concurrent.CompletableFuture.completedFuture(0L);
                  }

                  boolean shouldDelete = evaluateCondition(finalCondition, value, finalConditionValue);

                  if (shouldDelete) {
                     return handler.cache().removeAsync(key).thenApply(prev -> prev != null ? 1L : 0L);
                  } else {
                     return java.util.concurrent.CompletableFuture.completedFuture(0L);
                  }
               }),
               ctx,
               ResponseWriter.INTEGER
         );
      }
   }

   private boolean evaluateCondition(Condition condition, byte[] value, byte[] conditionValue) {
      switch (condition) {
         case IFEQ:
            return Arrays.equals(value, conditionValue);
         case IFNE:
            return !Arrays.equals(value, conditionValue);
         case IFDEQ:
            long valueDigest = XXH3.hash64(value);
            long targetDigest = XXH3.parseHexDigest(new String(conditionValue, StandardCharsets.US_ASCII));
            return valueDigest == targetDigest;
         case IFDNE:
            long valueDigest2 = XXH3.hash64(value);
            long targetDigest2 = XXH3.parseHexDigest(new String(conditionValue, StandardCharsets.US_ASCII));
            return valueDigest2 != targetDigest2;
         default:
            return true;
      }
   }

   private static boolean equalsIgnoreCase(byte[] a, byte[] b) {
      if (a.length != b.length) return false;
      for (int i = 0; i < a.length; i++) {
         byte ai = a[i];
         byte bi = b[i];
         if (ai != bi) {
            // Convert to uppercase for comparison
            if (ai >= 'a' && ai <= 'z') ai -= 32;
            if (bi >= 'a' && bi <= 'z') bi -= 32;
            if (ai != bi) return false;
         }
      }
      return true;
   }

   private enum Condition {
      NONE, IFEQ, IFNE, IFDEQ, IFDNE
   }
}
