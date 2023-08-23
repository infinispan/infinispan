package org.infinispan.server.resp.commands.set;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SUNION implementation, see:
 * {@link} https://redis.io/commands/sunion/
 *
 * @since 15.0
 */
public class SUNION extends RespCommand implements Resp3Command {
   public SUNION() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();

      var uniqueKeys = SINTER.getUniqueKeys(handler, arguments);
      var allEntries = esc.getAll(uniqueKeys);
      return handler.stageToReturn(
            allEntries.thenApply(sets -> union(sets.values())),
            ctx,
            Consumers.COLLECTION_BULK_BICONSUMER);
   }

   public static List<byte[]> union(Collection<SetBucket<byte[]>> sets) {
      var result = new ArrayList<byte[]>();
      for (SetBucket<byte[]> setBucket : sets) {
         if (setBucket != null) {
            for (byte[] el : setBucket.toSet()) {
               if (el == null) {
                  continue;
               }
               if (!result.stream().anyMatch((v)-> Objects.deepEquals(v, el))) {
                  result.add(el);
               }
            }
         }
      }
      return result;
   }
}
