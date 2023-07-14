package org.infinispan.server.resp.commands.pubsub;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/ubsubscribe/
 * @since 14.0
 */
public class UNSUBSCRIBE extends RespCommand implements Resp3Command, PubSubResp3Command {
   public static final String NAME = "UNSUBSCRIBE";

   public UNSUBSCRIBE() {
      super(NAME, -1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      if (arguments.size() == 0) {
         return handler.unsubscribeAll(ctx);
      } else {
         for (byte[] keyChannel : arguments) {
            WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
            SubscriberHandler.PubSubListener listener = handler.specificChannelSubscribers().remove(wrappedByteArray);
            if (listener != null) {
               aggregateCompletionStage.dependsOn(handler.handleStageListenerError(handler.cache().removeListenerAsync(listener), keyChannel, false));
            }
         }
      }
      return handler.sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, false);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      SubscriberHandler subscriberHandler = new SubscriberHandler(handler.respServer(), handler);
      return subscriberHandler.handleRequest(ctx, this, arguments);
   }
}
