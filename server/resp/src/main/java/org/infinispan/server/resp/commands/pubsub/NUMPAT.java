package org.infinispan.server.resp.commands.pubsub;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>PUBSUB NUMPAT</code>` command.
 * <p>
 * Return the number of unique pattern the current node is subscribed to. Only patterns are counted, that is, only
 * subscriptions executed with {@link PSUBSCRIBE} command.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/docs/latest/commands/pubsub-numpat/">Redis documentation.</a>
 * @author José Bolina
 */
class NUMPAT extends RespCommand implements Resp3Command {

   NUMPAT() {
      super(2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      CacheNotifier<?, ?> cn = SecurityActions.getCacheComponentRegistry(handler.cache()).getCacheNotifier().running();
      long patterns = cn.getListeners().stream()
            .filter(l -> l instanceof RespCacheListener)
            .map(l -> (RespCacheListener) l)
            .map(RespCacheListener::pattern)
            .filter(Objects::nonNull)
            .filter(PUBSUB.deduplicate())
            .count();
      Resp3Response.integers(patterns, handler.allocator());
      return handler.myStage();
   }
}
