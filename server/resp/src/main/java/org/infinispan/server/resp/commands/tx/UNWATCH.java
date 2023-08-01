package org.infinispan.server.resp.commands.tx;

import static org.infinispan.server.resp.commands.tx.WATCH.WATCHER_KEY;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>UNWATCH</code>` command.
 * <p>
 * Removes all the registered watchers in the current {@link ChannelHandlerContext}.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/unwatch/">Redis documentation.</a>
 * @author José Bolina
 */
public class UNWATCH extends RespCommand implements Resp3Command {

   public UNWATCH() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      CompletionStage<?> cs = deregister(ctx, handler.cache());
      return handler.stageToReturn(cs, ctx, Consumers.OK_BICONSUMER);
   }

   public static CompletionStage<List<WATCH.TxKeysListener>> deregister(ChannelHandlerContext ctx, AdvancedCache<byte[], byte[]> cache) {
      List<WATCH.TxKeysListener> watchers = ctx.channel().attr(WATCHER_KEY).getAndSet(null);
      if (watchers == null) {
         return CompletableFutures.completedNull();
      }

      AggregateCompletionStage<Void> stage = CompletionStages.aggregateCompletionStage();
      for (WATCH.TxKeysListener watcher : watchers) {
         stage.dependsOn(cache.removeListenerAsync(watcher));
      }

      return stage.freeze().thenApply(ignore -> watchers);
   }
}
