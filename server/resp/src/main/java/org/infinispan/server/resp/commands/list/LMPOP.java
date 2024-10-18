package org.infinispan.server.resp.commands.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.Util;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.jgroups.util.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;

/**
 * Pops one or more elements from the first non-empty list key from the list of provided key names.
 *
 * Elements are popped from either the left or right of the first non-empty list based on
 * the passed argument. The number of returned elements is limited to the lower between the
 * non-empty list's length, and the count argument (which defaults to 1).
 *
 * Returns array reply, specifically:
 * <ul>
 *    <li>null when no element could be popped.</li>
 *    <li>A two-element array with the first element being the name of the key from which elements were popped,
 *    and the second element is an array of elements.</li>
 * </ul>
 * @since 15.0
 * @see <a href="https://redis.io/commands/lmpop">Redis Documentation</a>
 */
public class LMPOP extends RespCommand implements Resp3Command {

   public static final byte[] COUNT = "COUNT".getBytes();
   public static final byte[] LEFT = "LEFT".getBytes();
   public static final byte[] RIGHT = "RIGHT".getBytes();

   public LMPOP() {
      super(-4, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int numKeys = 0;
      boolean invalidNumKeys = false;
      try {
         numKeys = ArgumentUtils.toInt(arguments.get(0));
         if (numKeys <= 0) {
            invalidNumKeys = true;
         }
      } catch (NumberFormatException nfe) {
         invalidNumKeys = true;
      }

      if (invalidNumKeys) {
         RespErrorUtil.customError("numkeys should be greater than 0", handler.allocator());
         return handler.myStage();
      }

      List<byte[]> listNames = new ArrayList<>(numKeys);
      int pos = 1;
      while (pos <= numKeys && pos < arguments.size()) {
         listNames.add(arguments.get(pos++));
      }

      if (pos < numKeys || pos > arguments.size()) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      byte[] leftOrRight = arguments.get(pos++);
      boolean isLeft = false;
      if (Util.isAsciiBytesEquals(LEFT, leftOrRight)) {
         isLeft = true;
      } else if (!Util.isAsciiBytesEquals(RIGHT, leftOrRight)) {
         // we need to chose between LEFT OR RIGHT
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }
      long count = 1;
      if (arguments.size() > pos ) {
         byte[] countArgValue;
         try {
            if (!Util.isAsciiBytesEquals(COUNT, arguments.get(pos++))) {
               throw new IllegalArgumentException("the value should be COUNT here");
            }
            countArgValue = arguments.get(pos++);
         } catch (Exception ex) {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }

         try {
            count = ArgumentUtils.toLong(countArgValue);
            if (count <= 0) {
               RespErrorUtil.customError("count should be greater than 0", handler.allocator());
               return handler.myStage();
            }
         } catch (Exception ex) {
            RespErrorUtil.syntaxError(handler.allocator());
            return handler.myStage();
         }
      }

      // if the user sends more arguments at this point, syntax error
      if (arguments.size() > pos) {
         RespErrorUtil.syntaxError(handler.allocator());
         return handler.myStage();
      }

      CompletionStage<PopResult> cs = asyncCalls(CompletableFutures.completedNull(), null, listNames.iterator(), count, isLeft, ctx, handler);
      return handler.stageToReturn(cs, ctx, Resp3Response.CUSTOM);
   }

   private CompletionStage<PopResult> asyncCalls(CompletionStage<Collection<byte[]>> pollValues,
                                                          byte[] prevName,
                                                          Iterator<byte[]> iteNames,
                                                          long count,
                                                          boolean isleft,
                                                          ChannelHandlerContext ctx,
                                                          Resp3Handler handler) {
      return pollValues.thenCompose(c -> {
         if (c != null){
            return CompletableFuture.completedFuture(new PopResult(prevName, c));
         }

         if (!iteNames.hasNext()) {
            return CompletableFutures.completedNull();
         }

         byte[] nextName = iteNames.next();
         return asyncCalls(handler.getListMultimap().poll(nextName, count, isleft), nextName, iteNames, count, isleft, ctx, handler);
      });
   }

   private record PopResult(byte[] key, Collection<byte[]> values) implements JavaObjectSerializer<PopResult> {

      @Override
      public void accept(PopResult ignore, ByteBufPool alloc) {
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
         Resp3Response.string(key, alloc);
         Resp3Response.array(values, alloc, Resp3Type.BULK_STRING);
      }
   }
}
