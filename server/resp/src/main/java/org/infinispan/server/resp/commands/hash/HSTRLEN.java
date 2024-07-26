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
 * `<code>HSTRLEN key field</code>` command.
 * </p>
 * the string length of the value associated with the field, or 0 if key or
 * field do not exist
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hstrlen/">Redis Documentation</a>
 * @author Vittorio Rigamonti
 */
public class HSTRLEN extends RespCommand implements Resp3Command {

   public HSTRLEN() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();

      return handler.stageToReturn(
            hashMap.get(arguments.get(0), arguments.get(1)).thenApply(v -> v == null ? 0 : v.length), ctx, Resp3Response.INTEGER);
   }
}
