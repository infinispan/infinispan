package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * Executes the `<code>HMSET key field value [field value ...]</code>` command.
 * <p>
 * Sets the specified `<code>field</code>`-`<code>value</code>` pairs in the hash stored at the given `<code>key</code>`.
 * </p>
 *
 * Note this command is deprecated since Redis 4.0 in favor of {@link HSET}.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hmset">Redis Documentation</a>
 */
public class HMSET extends RespCommand implements Resp3Command {

   public HMSET() {
      super(-4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      // Arguments are the hash map key and N key-value pairs.
      if ((arguments.size() & 1) == 0) {
         RespErrorUtil.wrongArgumentNumber(this, handler.allocator());
         return handler.myStage();
      }

      return CompletionStages.handleAndCompose(setEntries(handler, arguments), (ignore, t) -> {
         if (t != null) {
            return handleException(handler, t);
         }

         return handler.stageToReturn(CompletableFutures.completedNull(), ctx, Consumers.OK_BICONSUMER);
      });
   }

   protected CompletionStage<Integer> setEntries(Resp3Handler handler, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      Map.Entry<byte[], byte[]>[] entries = new Map.Entry[(arguments.size() - 1) >> 1];
      for (int i = 1; i < arguments.size(); i++) {
         entries[i / 2] = Map.entry(arguments.get(i), arguments.get(++i));
      }

      return hashMap.set(key, entries);
   }
}
