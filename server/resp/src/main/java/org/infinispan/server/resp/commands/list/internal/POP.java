package org.infinispan.server.resp.commands.list.internal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * Abstract class for common code on POP operations
 *
 * @since 15.0
 */
public abstract class POP extends RespCommand implements Resp3Command {
   private static final BiConsumer<Object, ResponseWriter> SERIALIZER = (res, writer) -> {
      if (res instanceof Collection<?>) {
         Collection<byte[]> strings = (Collection<byte[]>) res;
         writer.array(strings, Resp3Type.BULK_STRING);
         return;
      }

      writer.string((byte[]) res);
   };

   protected boolean first;

   public POP(boolean first) {
      super(-2, 1, 1, 1);
      this.first = first;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return popAndReturn(handler, ctx, arguments);
   }

   private CompletionStage<RespRequestHandler> popAndReturn(Resp3Handler handler,
                                                               ChannelHandlerContext ctx,
                                                               List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      final long count;
      switch (arguments.size()) {
         case 1:
            count = 1;
            break;
         case 2:
            count = ArgumentUtils.toLong(arguments.get(1));
            if (count < 0) {
               handler.writer().mustBePositive();
               return handler.myStage();
            }
            break;
         default:
            handler.writer().wrongArgumentNumber(this);
            return handler.myStage();
      }

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Collection<byte[]>> pollValues = first ?
            listMultimap.pollFirst(key, count) :
            listMultimap.pollLast(key, count);

      CompletionStage<Object> cs = pollValues
            .thenApply(c -> {
               if (c == null) return null;

               // one single element and count argument was not present, return the value as string
               if (c.size() == 1 && arguments.size() == 1)
                  return c.iterator().next();

               // return an array of strings
               return c;
            });

      return handler.stageToReturn(cs, ctx, SERIALIZER);
   }
}
