package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HGET key field</code>` command.
 * </p>
 * Find the value associated with field in the hash stored at key.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hget/">Redis Documentation</a>
 * @author José Bolina
 */
public class HGET extends RespCommand implements Resp3Command {

   public HGET() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      return handler.stageToReturn(hashMap.get(arguments.get(0), arguments.get(1)), ctx, Consumers.BULK_BICONSUMER);
   }
}
