package org.infinispan.server.resp.commands.set;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

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
            Resp3Response.SET_BULK_STRING);
   }

   public static Set<byte[]> union(Collection<SetBucket<byte[]>> sets) {
      Set<byte[]> result = new HashSet<>();
      for (SetBucket<byte[]> setBucket : sets) {
         if (setBucket != null) {
            for (byte[] el : setBucket.toSet()) {
               if (el == null) {
                  continue;
               }
               if (result.stream().noneMatch((v)-> Objects.deepEquals(v, el))) {
                  result.add(el);
               }
            }
         }
      }
      return result;
   }
}
