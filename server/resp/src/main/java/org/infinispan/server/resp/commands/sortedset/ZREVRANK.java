package org.infinispan.server.resp.commands.sortedset;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * ZREVRANK
 *
 * @see <a href="https://redis.io/commands/zrevrank/">ZREVRANK</a>
 * @since 15.0
 */
public class ZREVRANK extends ZRANK {
   public ZREVRANK() {
      super();
      this.isRev = true;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return super.perform(handler, ctx, arguments);
   }
}
