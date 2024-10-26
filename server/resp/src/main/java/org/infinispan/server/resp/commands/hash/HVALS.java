package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * HVALS
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hvals/">HVALS</a>
 * @since 15.0
 */
public class HVALS extends RespCommand implements Resp3Command {

   public HVALS() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      return handler.stageToReturn(multimap.values(arguments.get(0)), ctx, Resp3Response.ARRAY_BULK_STRING);
   }
}
