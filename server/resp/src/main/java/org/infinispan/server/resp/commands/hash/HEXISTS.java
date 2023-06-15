package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HEXISTS key field</code>` command.
 * <p>
 *    Verify if the specified <code>field</code> exists in the hash stored at <code>key</code>. Returns 1 if true,
 *    or 0, otherwise.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hexists">Redis Documentation</a>
 * @author José Bolina
 */
public class HEXISTS extends RespCommand implements Resp3Command {

   private static final Function<Boolean, Long> CONVERTER = b -> b ? 1L : 0L;

   public HEXISTS() {
      super(3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Long> cs = multimap.contains(arguments.get(0), arguments.get(1)).thenApply(CONVERTER);
      return handler.stageToReturn(cs, ctx, Consumers.LONG_BICONSUMER);
   }
}
