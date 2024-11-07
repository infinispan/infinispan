package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HEXISTS
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hexists/">HEXISTS</a>
 * @since 15.0
 */
public class HEXISTS extends RespCommand implements Resp3Command {

   static final Function<Boolean, Long> CONVERTER = b -> b ? 1L : 0L;

   public HEXISTS() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Long> cs = multimap.contains(arguments.get(0), arguments.get(1)).thenApply(CONVERTER);
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }
}
