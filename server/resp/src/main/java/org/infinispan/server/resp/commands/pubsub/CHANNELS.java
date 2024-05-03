package org.infinispan.server.resp.commands.pubsub;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.infinispan.commons.util.GlobUtils;
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
 * @link <a href="https://redis.io/docs/latest/commands/pubsub-channels/">Redis documentation.</a>
 * @author José Bolina
 */
public class CHANNELS extends RespCommand implements Resp3Command {

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
            .filter(filter)
            .collect(Collectors.filtering(deduplicate(), Collectors.toList()));

      Consumers.COLLECTION_BULK_BICONSUMER.accept(channels, handler.allocator());
      return handler.myStage();
   }

   private Predicate<byte[]> globFilter(byte[] glob) {
      Pattern pattern = Pattern.compile(GlobUtils.globToRegex(new String(glob, StandardCharsets.US_ASCII)));
      return channel -> {
         String converted = new String(channel, StandardCharsets.US_ASCII);
         return pattern.matcher(converted).matches();
      };
   }

   private Predicate<byte[]> deduplicate() {
      List<byte[]> channels = new ArrayList<>(4);
      return channel -> {
         for (byte[] bytes : channels) {
            if (Arrays.equals(channel, bytes))
               return false;
         }
         channels.add(channel);
         return true;
      };
   }
}
