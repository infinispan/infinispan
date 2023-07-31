package org.infinispan.server.resp.commands.pubsub;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * @link https://redis.io/commands/publish/
 * @since 14.0
 */
public class PUBLISH extends RespCommand implements Resp3Command {
   public PUBLISH() {
      super(3, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      // TODO: should we return the # of subscribers on this node?
      // We use expiration to remove the event values eventually while preventing them during high periods of
      // updates
      return handler.stageToReturn(handler.ignorePreviousValuesCache()
            .putAsync(KeyChannelUtils.keyToChannel(arguments.get(0)), arguments.get(1), 3, TimeUnit.SECONDS), ctx,
            (ignore, alloc) -> ByteBufferUtils.stringToByteBufAscii(":0\r\n", alloc)
      );
   }
}
