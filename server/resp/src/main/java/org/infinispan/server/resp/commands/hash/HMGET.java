package org.infinispan.server.resp.commands.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HMGET
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hmget/">HMGET</a>
 * @since 15.0
 */
public class HMGET extends RespCommand implements Resp3Command {

   public HMGET() {
      super(-3, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.HASH | AclCategory.FAST;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Collection<byte[]>> cs = multimap.get(arguments.get(0), arguments.subList(1, arguments.size()).toArray(Util.EMPTY_BYTE_ARRAY_ARRAY))
            .thenApply(this::wrapKeys)
            .thenApply(entries -> {
               List<byte[]> result = new ArrayList<>(arguments.size() - 1);
               for (byte[] argument : arguments.subList(1, arguments.size())) {
                  result.add(entries.get(new WrappedByteArray(argument)));
               }
               return result;
            });
      return handler.stageToReturn(cs, ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   private Map<WrappedByteArray, byte[]> wrapKeys(Map<byte[], byte[]> original) {
      return original.entrySet().stream()
            .collect(HashMap::new, (m, v) -> m.put(new WrappedByteArray(v.getKey()), v.getValue()), HashMap::putAll);
   }
}
