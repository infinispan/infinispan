package org.infinispan.server.resp.commands.hash;

import java.util.ArrayList;
import java.util.Collection;
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
 * `<code>HMGET key field [field ...]</code>` command.
 * <p>
 * Returns the values associated with all <code>field</code>s in the hash stored at <code>key</code>, returning a list
 * with the values in the same order of the <code>field</code>s. No error returns from <code>field</code>s or
 * <code>key</code> that do not exist in the hash, and <code>null</code> is returned in the position.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hmget/">Redis Documentation</a>
 * @author José Bolina
 */
public class HMGET extends RespCommand implements Resp3Command {

   public HMGET() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Collection<byte[]>> cs = multimap.get(arguments.get(0), arguments.subList(1, arguments.size()).toArray(new byte[0][]))
            .thenApply(entries -> {
               List<byte[]> result = new ArrayList<>(arguments.size() - 1);
               for (byte[] argument : arguments.subList(1, arguments.size())) {
                  result.add(entries.get(argument));
               }
               return result;
            });
      return handler.stageToReturn(cs, ctx, Consumers.GET_ARRAY_BICONSUMER);
   }
}
