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

import io.netty.channel.ChannelHandlerContext;

/**
 * NUMPAT
 *
 * @author José Bolina
 * @see <a href="https://redis.io/docs/latest/commands/pubsub-numpat/">PUBSUB NUMPAT</a>
 * @since 15.0
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
      handler.writer().integers(patterns);
      return handler.myStage();
   }
}
