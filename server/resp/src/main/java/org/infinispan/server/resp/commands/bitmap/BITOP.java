package org.infinispan.server.resp.commands.bitmap;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import io.netty.channel.ChannelHandlerContext;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * BITOP
 *
 * @see <a href="https://redis.io/commands/bitop/">BITOP</a>
 * @since 16.2
 */
public class BITOP extends RespCommand implements Resp3Command {
   public BITOP() {
      super(-4, 2, -1, 1, AclCategory.WRITE.mask() | AclCategory.BITMAP.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String operation = new String(arguments.get(0)).toUpperCase();
      byte[] destKey = arguments.get(1);
      List<byte[]> srcKeys = arguments.subList(2, arguments.size());

      CompletionStage<List<byte[]>> readStage = CompletionStages.performConcurrently(
            srcKeys, ProcessorInfo.availableProcessors(),
            Schedulers.from(new WithinThreadExecutor()),
            k -> handler.cache().getAsync(k)
                  .thenApply(v -> v != null ? v : new byte[0]), Collectors.toList());

      CompletionStage<Long> cs = readStage.thenCompose(values -> {
         byte[] result = switch (operation) {
            case "AND", "OR", "XOR" -> bitop(operation, values);
            case "NOT" -> {
               if (values.size() != 1) {
                  throw new IllegalArgumentException("BITOP NOT must be called with a single source key.");
               }
               yield not(values.get(0));
            }
            case "DIFF" -> diff(values);
            case "DIFF1" -> diff1(values);
            case "ANDOR" -> andor(values);
            case "ONE" -> one(values);
            default -> throw new IllegalArgumentException("ERR syntax error");
         };
         return handler.cache().putAsync(destKey, result)
               .thenApply(r -> (long) result.length);
      });
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }

   private byte[] not(byte[] value) {
      if (value == null) {
         return new byte[0];
      }
      byte[] result = new byte[value.length];
      for (int i = 0; i < value.length; i++) {
         result[i] = (byte) ~value[i];
      }
      return result;
   }

   private int maxLength(List<byte[]> values) {
      int maxLength = 0;
      for (byte[] value : values) {
         if (value != null && value.length > maxLength) {
            maxLength = value.length;
         }
      }
      return maxLength;
   }

   private byte getByteAt(byte[] value, int i) {
      return (value != null && i < value.length) ? value[i] : 0;
   }

   // X AND NOT(Y1 OR Y2 OR ... OR Yn)
   private byte[] diff(List<byte[]> values) {
      int maxLength = maxLength(values);
      if (maxLength == 0) return new byte[0];

      byte[] x = values.get(0);
      List<byte[]> ys = values.subList(1, values.size());

      byte[] result = new byte[maxLength];
      for (int i = 0; i < maxLength; i++) {
         byte orY = 0;
         for (byte[] y : ys) {
            orY |= getByteAt(y, i);
         }
         result[i] = (byte) (getByteAt(x, i) & ~orY);
      }
      return result;
   }

   // (Y1 OR Y2 OR ... OR Yn) AND NOT(X)
   private byte[] diff1(List<byte[]> values) {
      int maxLength = maxLength(values);
      if (maxLength == 0) return new byte[0];

      byte[] x = values.get(0);
      List<byte[]> ys = values.subList(1, values.size());

      byte[] result = new byte[maxLength];
      for (int i = 0; i < maxLength; i++) {
         byte orY = 0;
         for (byte[] y : ys) {
            orY |= getByteAt(y, i);
         }
         result[i] = (byte) (orY & ~getByteAt(x, i));
      }
      return result;
   }

   // X AND (Y1 OR Y2 OR ... OR Yn)
   private byte[] andor(List<byte[]> values) {
      int maxLength = maxLength(values);
      if (maxLength == 0) return new byte[0];

      byte[] x = values.get(0);
      List<byte[]> ys = values.subList(1, values.size());

      byte[] result = new byte[maxLength];
      for (int i = 0; i < maxLength; i++) {
         byte orY = 0;
         for (byte[] y : ys) {
            orY |= getByteAt(y, i);
         }
         result[i] = (byte) (getByteAt(x, i) & orY);
      }
      return result;
   }

   // Bit set in exactly one of the source keys
   private byte[] one(List<byte[]> values) {
      int maxLength = maxLength(values);
      if (maxLength == 0) return new byte[0];

      byte[] result = new byte[maxLength];
      for (int i = 0; i < maxLength; i++) {
         byte one = 0;
         byte more = 0;
         for (byte[] value : values) {
            byte v = getByteAt(value, i);
            more |= (byte) (one & v);
            one ^= v;
         }
         result[i] = (byte) (one & ~more);
      }
      return result;
   }

   private byte[] bitop(String op, List<byte[]> values) {
      int maxLength = maxLength(values);
      if (maxLength == 0) return new byte[0];

      byte[] result = new byte[maxLength];
      for (int i = 0; i < maxLength; i++) {
         byte b = 0;
         boolean first = true;
         for (byte[] value : values) {
            byte v = getByteAt(value, i);
            if (first) {
               b = v;
               first = false;
               continue;
            }
            switch (op) {
               case "AND":
                  b &= v;
                  break;
               case "OR":
                  b |= v;
                  break;
               case "XOR":
                  b ^= v;
                  break;
            }
         }
         result[i] = b;
      }
      return result;
   }
}
