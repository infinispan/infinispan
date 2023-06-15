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
 * `<code>HVALS key</code>` command.
 * <p>
 *    Return all values in the hash map stored under <code>key</code>.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hvals/">Redis Documentation</a>
 * @author José Bolina
 */
public class HVALS extends RespCommand implements Resp3Command {

   public HVALS() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      return handler.stageToReturn(multimap.values(arguments.get(0)), ctx, Consumers.GET_ARRAY_BICONSUMER);
   }
}
