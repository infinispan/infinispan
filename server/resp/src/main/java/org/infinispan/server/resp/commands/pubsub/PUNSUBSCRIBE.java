package org.infinispan.server.resp.commands.pubsub;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.meta.ClientMetadata;

import io.netty.channel.ChannelHandlerContext;

/**
 * PUNSUBSCRIBE
 *
 * @see <a href="https://redis.io/commands/punsubscribe/">PUNSUBSCRIBE</a>
 * @since 14.0
 */
public class PUNSUBSCRIBE extends RespCommand implements Resp3Command, PubSubResp3Command {
   public static final String NAME = "PUNSUBSCRIBE";

   public PUNSUBSCRIBE() {
      super(NAME, -1, 0, 0, 0, AclCategory.PUBSUB.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      if (arguments.isEmpty()) {
         return handler.punsubscribeAll(ctx);
      } else {
         ClientMetadata metadata = handler.respServer().metadataRepository().client();
         for (byte[] patternArg : arguments) {
            WrappedByteArray wrappedByteArray = new WrappedByteArray(patternArg);
            RespCacheListener listener = handler.patternSubscribers().remove(wrappedByteArray);
            if (listener != null) {
               aggregateCompletionStage.dependsOn(handler.handleStageListenerError(handler.cache().removeListenerAsync(listener), patternArg, false));
               metadata.decrementPubSubClients();
            }
         }
      }
      return handler.sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, false, true);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      SubscriberHandler subscriberHandler = new SubscriberHandler(handler.respServer(), handler);
      return subscriberHandler.handleRequest(ctx, this, arguments);
   }
}
