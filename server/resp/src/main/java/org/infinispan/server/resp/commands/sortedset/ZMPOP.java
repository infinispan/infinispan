package org.infinispan.server.resp.commands.sortedset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.response.ScoredValueSerializer;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;
import org.jgroups.util.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;

/**
 * Pops one or more elements, that are member-score pairs, from the first non-empty sorted set in the
 * provided list of key names.
 *
 * {@link ZMPOP} and {BZMPOP} are similar to the following, more limited, commands:
 *
 * {@link ZPOPMIN} or {@link ZPOPMAX} which take only one key, and can return multiple elements.
 * {BZPOPMIN} or {BZPOPMAX} which take multiple keys, but return only one element from just one key.
 *
 * See {BZMPOP} for the blocking variant of this command.
 * @since 15.0
 * @see <a href="https://redis.io/commands/zpopmax/">Redis Documentation</a>
 */
public class ZMPOP extends RespCommand implements Resp3Command {

   private static final byte[] MIN = "MIN".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] MAX = "MAX".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);

   public ZMPOP() {
      super(-4, 0, 0, 0);
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
      } catch (Exception ex) {
      }

      if (numberKeys <= 0) {
         RespErrorUtil.customError("numkeys should be greater than 0", handler.allocator());
         return handler.myStage();
      }

      List<byte[]> sortedSetNames = new ArrayList<>();
      while (sortedSetNames.size() < numberKeys && pos < arguments.size()) {
         sortedSetNames.add(arguments.get(pos++));
      }

      if (sortedSetNames.size() != numberKeys || pos >= arguments.size()) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }
      byte[] minOrMax = arguments.get(pos++);
      final boolean isMin;
      if (Util.isAsciiBytesEquals(MIN, minOrMax)) {
         isMin = true;
      } else if (Util.isAsciiBytesEquals(MAX, minOrMax)) {
         isMin = false;
      } else {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      int count = 1;
      if (pos < arguments.size()) {
         byte[] countArg = arguments.get(pos++);
         if (Util.isAsciiBytesEquals(COUNT, countArg) && pos < arguments.size()) {
            try {
               byte[] bytes = arguments.get(pos++);
               count = ArgumentUtils.toInt(bytes);
            } catch (Exception ex) {
               count = -1;
            }
            if (count <= 0) {
               RespErrorUtil.customError("count should be greater than 0", handler.allocator());
               return handler.myStage();
            }
         } else {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
      }

      if (arguments.size() > pos) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      CompletionStage<PopResult> cs = asyncCalls(CompletableFutures.completedNull(), null, sortedSetNames.iterator(), count, isMin, ctx, handler);
      return handler.stageToReturn(cs, ctx, (res, alloc) -> Resp3Response.write(res, alloc, res));
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

   private record PopResult(byte[] name, Collection<ScoredValue<byte[]>> values) implements JavaObjectSerializer<PopResult> {

      @Override
      public void accept(PopResult res, ByteBufPool alloc) {
         // Response written as an array of two elements.
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
         Resp3Response.string(name, alloc);
         Resp3Response.array(values, alloc, ScoredValueSerializer.INSTANCE);
      }
   }
}
