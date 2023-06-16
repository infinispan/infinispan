package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * Executes the `<code>HSET key field value [field value ...]</code>` command.
 * <p>
 * Sets the specified `<code>field</code>`-`<code>value</code>` pairs in the hash stored at the given `<code>key</code>`.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hset">Redis Documentation</a>
 */
public class HSET extends HMSET {

   static final BiConsumer<Integer, ByteBufPool> CONSUMER = (r, t) -> Consumers.LONG_BICONSUMER.accept((long) r, t);

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      // Arguments are the hash map key and N key-value pairs.
      if ((arguments.size() & 1) == 0) {
         RespErrorUtil.wrongArgumentNumber(this, handler.allocator());
         return handler.myStage();
      }

      return handler.stageToReturn(setEntries(handler, arguments), ctx, CONSUMER);
   }
}
