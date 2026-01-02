package org.infinispan.server.resp.commands.sortedset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.response.ScoredValueSerializer;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.jgroups.util.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZMPOP
 *
 * @see <a href="https://redis.io/commands/zmpop/">ZMPOP</a>
 * @since 15.0
 */
public class ZMPOP extends RespCommand implements Resp3Command {

   private static final byte[] MIN = "MIN".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAX = "MAX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);

   public ZMPOP() {
      super(-4, 0, 0, 0, AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      int pos = 0;
      int numberKeys = -1;
      try {
         byte[] bytes = arguments.get(pos++);
         numberKeys = ArgumentUtils.toInt(bytes);
      } catch (Exception ignore) {
      }

      if (numberKeys <= 0) {
         handler.writer().customError("numkeys should be greater than 0");
         return handler.myStage();
      }

      List<byte[]> sortedSetNames = new ArrayList<>();
      while (sortedSetNames.size() < numberKeys && pos < arguments.size()) {
         sortedSetNames.add(arguments.get(pos++));
      }

      if (sortedSetNames.size() != numberKeys || pos >= arguments.size()) {
         handler.writer().syntaxError();
         return handler.myStage();
      }
      byte[] minOrMax = arguments.get(pos++);
      final boolean isMin;
      if (RespUtil.isAsciiBytesEquals(MIN, minOrMax)) {
         isMin = true;
      } else if (RespUtil.isAsciiBytesEquals(MAX, minOrMax)) {
         isMin = false;
      } else {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      int count = 1;
      if (pos < arguments.size()) {
         byte[] countArg = arguments.get(pos++);
         if (RespUtil.isAsciiBytesEquals(COUNT, countArg) && pos < arguments.size()) {
            try {
               byte[] bytes = arguments.get(pos++);
               count = ArgumentUtils.toInt(bytes);
            } catch (Exception ex) {
               count = -1;
            }
            if (count <= 0) {
               handler.writer().customError("count should be greater than 0");
               return handler.myStage();
            }
         } else {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      }

      if (arguments.size() > pos) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      CompletionStage<PopResult> cs = asyncCalls(CompletableFutures.completedNull(), null, sortedSetNames.iterator(), count, isMin, ctx, handler);
      return handler.stageToReturn(cs, ctx, (res, writer) -> writer.write(res, res));
   }

   private CompletionStage<PopResult> asyncCalls(CompletionStage<Collection<ScoredValue<byte[]>>> popValues,
                                                 byte[] prevName,
                                                 Iterator<byte[]> iteNames,
                                                 long count,
                                                 boolean isMin,
                                                 ChannelHandlerContext ctx,
                                                 Resp3Handler handler) {

      return popValues.thenApply(c -> {
         if (c != null && !c.isEmpty()) {
            return new PopResult(prevName, c);
         }
         return null;
      }).thenCompose(res -> {
         if (res != null) return CompletableFuture.completedFuture(res);
         if (!iteNames.hasNext()) return CompletableFutures.completedNull();

         byte[] nextName = iteNames.next();
         return asyncCalls(handler.getSortedSeMultimap().pop(nextName, isMin, count), nextName, iteNames, count, isMin, ctx, handler);
      });
   }

   private record PopResult(byte[] name,
                            Collection<ScoredValue<byte[]>> values) implements JavaObjectSerializer<PopResult> {

      @Override
      public void accept(PopResult res, ResponseWriter writer) {
         // Response written as an array of two elements.
         writer.array(List.of(name, values), (o, w) -> {
            if (o instanceof Collection<?>) {
               w.array((Collection<ScoredValue<byte[]>>) o, ScoredValueSerializer.WITH_SCORE);
            } else {
               w.string((byte[]) o);
            }
         });
      }
   }
}
