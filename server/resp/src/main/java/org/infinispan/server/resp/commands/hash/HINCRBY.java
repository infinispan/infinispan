package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

public class HINCRBY extends RespCommand implements Resp3Command {

   private static final BiConsumer<byte[], ByteBufPool> CONVERTER =
         (value, pool) -> Consumers.LONG_BICONSUMER.accept(ArgumentUtils.toLong(value), pool);

   public HINCRBY() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      Number delta = ArgumentUtils.toNumber(arguments.get(2));
      CompletionStage<byte[]> cs = multimap.compute(arguments.get(0), arguments.get(1), (ignore, prev) -> {
         if (prev == null) return arguments.get(2);

         // This is supposed to throw when the `prev` is not a long.
         long prevLong = ArgumentUtils.toLong(prev);
         if (delta instanceof Double) {
            return ArgumentUtils.toByteArray(prevLong + delta.doubleValue());
         }

         return ArgumentUtils.toByteArray(prevLong + delta.longValue());
      });
      return handler.stageToReturn(cs, ctx, CONVERTER);
   }
}
