package org.infinispan.server.resp.commands.list.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * Abstract class for common code on PUSH operations
 *
 * @since 15.0
 */
public abstract class PUSH extends RespCommand implements Resp3Command {
   protected boolean first;

   public PUSH(boolean first) {
      super(-3, 1, 1, 1);
      this.first = first;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      return handler.stageToReturn(pushAndReturn(handler, arguments), ctx, Consumers.LONG_BICONSUMER);
   }

   protected CompletionStage<Long> pushAndReturn(Resp3Handler handler, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Void> push = first ? listMultimap.offerFirst(key, arguments.subList(1, arguments.size())) : listMultimap.offerLast(key, arguments.subList(1, arguments.size()));
      return push.thenCompose(ignore -> listMultimap.size(key));
   }
}
