package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HSET
 *
 * @see <a href="https://redis.io/commands/hset/">HSET</a>
 * @since 15.0
 */
public class HSET extends HMSET {

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      // Arguments are the hash map key and N key-value pairs.
      if ((arguments.size() & 1) == 0) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      return handler.stageToReturn(setEntries(handler, arguments), ctx, ResponseWriter.INTEGER);
   }
}
