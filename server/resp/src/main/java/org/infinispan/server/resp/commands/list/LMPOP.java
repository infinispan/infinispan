package org.infinispan.server.resp.commands.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.jgroups.util.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;

/**
 * LMPOP
 *
 * @see <a href="https://redis.io/commands/lmpop/">LMPOP</a>
 * @since 15.0
 */
public class LMPOP extends RespCommand implements Resp3Command {

   public static final byte[] COUNT = "COUNT".getBytes();
   public static final byte[] LEFT = "LEFT".getBytes();
   public static final byte[] RIGHT = "RIGHT".getBytes();

   public LMPOP() {
      super(-4, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.LIST | AclCategory.SLOW;
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
         handler.writer().customError("numkeys should be greater than 0");
         return handler.myStage();
      }

      List<byte[]> listNames = new ArrayList<>(numKeys);
      int pos = 1;
      while (pos <= numKeys && pos < arguments.size()) {
         listNames.add(arguments.get(pos++));
      }

      if (pos < numKeys || pos > arguments.size()) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      byte[] leftOrRight = arguments.get(pos++);
      boolean isLeft = false;
      if (RespUtil.isAsciiBytesEquals(LEFT, leftOrRight)) {
         isLeft = true;
      } else if (!RespUtil.isAsciiBytesEquals(RIGHT, leftOrRight)) {
         // we need to chose between LEFT OR RIGHT
         handler.writer().syntaxError();
         return handler.myStage();
      }
      long count = 1;
      if (arguments.size() > pos) {
         byte[] countArgValue;
         try {
            if (!RespUtil.isAsciiBytesEquals(COUNT, arguments.get(pos++))) {
               throw new IllegalArgumentException("the value should be COUNT here");
            }
            countArgValue = arguments.get(pos++);
         } catch (Exception ex) {
            handler.writer().syntaxError();
            return handler.myStage();
         }

         try {
            count = ArgumentUtils.toLong(countArgValue);
            if (count <= 0) {
               handler.writer().customError("count should be greater than 0");
               return handler.myStage();
            }
         } catch (Exception ex) {
            handler.writer().syntaxError();
            return handler.myStage();
         }
      }

      // if the user sends more arguments at this point, syntax error
      if (arguments.size() > pos) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      CompletionStage<PopResult> cs = asyncCalls(CompletableFutures.completedNull(), null, listNames.iterator(), count, isLeft, ctx, handler);
      return handler.stageToReturn(cs, ctx, ResponseWriter.CUSTOM);
   }

   private CompletionStage<PopResult> asyncCalls(CompletionStage<Collection<byte[]>> pollValues,
                                                 byte[] prevName,
                                                 Iterator<byte[]> iteNames,
                                                 long count,
                                                 boolean isleft,
                                                 ChannelHandlerContext ctx,
                                                 Resp3Handler handler) {
      return pollValues.thenCompose(c -> {
         if (c != null) {
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
      public void accept(PopResult ignore, ResponseWriter writer) {
         writer.array(List.of(key, values), (o, w) -> {
            if (o instanceof Collection<?>) {
               w.array((Collection<byte[]>) o, Resp3Type.BULK_STRING);
            } else {
               w.string((byte[]) o);
            }
         });
      }
   }
}
