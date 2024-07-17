package org.infinispan.server.resp.commands.pubsub;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.util.GlobMatcher;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>PUBSUB CHANNELS [pattern]</code>` command.
 * <p>
 * List the existing subscribers matching the optional glob pattern. If no pattern is specified, all channels are listed.
 * The reply in a clustered environment is local to the node handling the command.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/docs/latest/commands/pubsub-channels/">Redis documentation.</a>
 * @author José Bolina
 */
class CHANNELS extends RespCommand implements Resp3Command {

   private static final Predicate<byte[]> PASS_ALL = ignore -> true;

   CHANNELS() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      CacheNotifier<?, ?> cn = SecurityActions.getCacheComponentRegistry(handler.cache()).getCacheNotifier().running();
      Predicate<byte[]> filter = PASS_ALL;

      if (arguments.size() == 2) {
         filter = globFilter(arguments.get(1));
      }

      // Retrieve all existing cache listeners for this node.
      // We must return the active channels. One channel can have multiple listeners from different clients.
      // We filter out the listeners by comparing the channel bytes.
      Collection<byte[]> channels = cn.getListeners().stream()
            .filter(l -> l instanceof RespCacheListener)
            .map(l -> (RespCacheListener) l)
            .map(RespCacheListener::subscribedChannel)
            .filter(Objects::nonNull)
            .filter(filter)
            .collect(Collectors.filtering(PUBSUB.deduplicate(), Collectors.toList()));

      Consumers.COLLECTION_BULK_BICONSUMER.accept(channels, handler.allocator());
      return handler.myStage();
   }

   private Predicate<byte[]> globFilter(byte[] glob) {
      return channel -> GlobMatcher.match(glob, channel);
   }
}
