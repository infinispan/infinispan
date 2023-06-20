package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HINCRBYFLOAT key field increment</code>` command.
 * <p>
 * Increments the value at the <code>field</code> in the hash map stored at <code>key</code> by <code>increment</code>.
 * The command fails if the stored value is not a number.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hincrbyfloat/">Redis Documentation</a>
 * @author José Bolina
 */
public class HINCRBYFLOAT extends RespCommand implements Resp3Command {

   public HINCRBYFLOAT() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      double delta = ArgumentUtils.toDouble(arguments.get(2));
      CompletionStage<byte[]> cs = multimap.compute(arguments.get(0), arguments.get(1), (ignore, prev) -> {
         if (prev == null) return arguments.get(2);

         double prevDouble = ArgumentUtils.toDouble(prev);
         return ArgumentUtils.toByteArray(prevDouble + delta);
      });
      return handler.stageToReturn(cs, ctx, Consumers.GET_BICONSUMER);
   }
}
