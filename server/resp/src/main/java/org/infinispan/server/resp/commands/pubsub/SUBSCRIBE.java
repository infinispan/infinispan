package org.infinispan.server.resp.commands.pubsub;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.encoding.DataConversion;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.meta.ClientMetadata;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

/**
 * SUBSCRIBE
 *
 * @see <a href="https://redis.io/commands/subscribe/">SUBSCRIBE</a>
 * @since 14.0
 */
public class SUBSCRIBE extends RespCommand implements Resp3Command, PubSubResp3Command {
   private static final Log log = Log.getLog(SUBSCRIBE.class);

   public SUBSCRIBE() {
      super(-2, 0, 0, 0, AclCategory.PUBSUB.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      SubscriberHandler subscriberHandler = new SubscriberHandler(handler.respServer(), handler);
      return subscriberHandler.handleRequest(ctx, this, arguments);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ClientMetadata metadata = handler.respServer().metadataRepository().client();
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      for (byte[] keyChannel : arguments) {
         if (log.isTraceEnabled()) {
            log.tracef("Subscriber for channel: " + CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
         }
         WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
         if (handler.specificChannelSubscribers().get(wrappedByteArray) == null) {
            RespCacheListener pubSubListener = SubscriberHandler.newKeyListener(ctx.channel(), keyChannel);
            handler.specificChannelSubscribers().put(wrappedByteArray, pubSubListener);
            byte[] channel = KeyChannelUtils.keyToChannel(keyChannel);
            DataConversion dc = handler.cache().getValueDataConversion();
            CompletionStage<Void> stage = handler.cache().addListenerAsync(pubSubListener, new EventListenerKeysFilter(channel), new EventListenerConverter<Object,Object,byte[]>(dc));
            aggregateCompletionStage.dependsOn(handler.handleStageListenerError(stage, keyChannel, true));
            metadata.incrementPubSubClients();
         }
      }
      return handler.sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, true);
   }
}
