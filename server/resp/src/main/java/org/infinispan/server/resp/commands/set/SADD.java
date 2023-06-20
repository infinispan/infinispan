package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * Abstract class for common code on SADD operations
 *
 * @since 15.0
 */
public class SADD extends RespCommand implements Resp3Command {
   public SADD() {
      super(-3, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      EmbeddedSetCache<byte[],byte[]> esc = handler.getEmbeddedSetCache();
      CompletionStage<Long> result = esc.add(key, arguments.subList(1, arguments.size()));
      return handler.stageToReturn(result, ctx, Consumers.LONG_BICONSUMER);
   }
}
