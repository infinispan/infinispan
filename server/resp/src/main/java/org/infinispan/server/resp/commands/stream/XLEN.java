package org.infinispan.server.resp.commands.stream;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * XLEN
 *
 * @see <a href="https://redis.io/commands/xlen/">XLEN</a>
 * @since 16.4
 */
public class XLEN extends RespCommand implements Resp3Command {

   public XLEN() {
      super(2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.STREAM.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      return handler.stageToReturn(handler.getStreamCache().size(key), ctx, ResponseWriter.INTEGER);
   }
}
