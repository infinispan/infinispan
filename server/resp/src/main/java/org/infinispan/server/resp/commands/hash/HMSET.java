package org.infinispan.server.resp.commands.hash;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HMSET
 *
 * @see <a href="https://redis.io/commands/hmset/">HMSET</a>
 * @since 15.0
 */
public class HMSET extends RespCommand implements Resp3Command {

   public HMSET() {
      super(-4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      // Arguments are the hash map key and N key-value pairs.
      if ((arguments.size() & 1) == 0) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      return handler.stageToReturn(setEntries(handler, arguments), ctx, ResponseWriter.OK);
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
