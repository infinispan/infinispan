package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * SRANDMEMBER
 *
 * @see <a href="https://redis.io/commands/srandmember/">SRANDMEMBER</a>
 * @since 15.0
 */
public class SRANDMEMBER extends RespCommand implements Resp3Command {
   public SRANDMEMBER() {
      super(-2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      final long count = (arguments.size() <= 1) ? 1 : ArgumentUtils.toLong(arguments.get(1));
      if (count == Long.MIN_VALUE) {
         RespErrorUtil.customError("value is out of range, value must between -9223372036854775807 and 9223372036854775807", handler.allocator());
         return handler.myStage();
      }
      final byte[] key = arguments.get(0);
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      return handler.stageToReturn(esc.pop(key, count, false), ctx, Resp3Response.ARRAY_BULK_STRING);
   }
}
