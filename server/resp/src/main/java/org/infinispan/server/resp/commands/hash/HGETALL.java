package org.infinispan.server.resp.commands.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>HGETALL key</code>` command.
 * <p>
 *    Returns the key-value pairs in the hash map stored at the given key.
 * </p>
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/hgetall/">Redis Documentation</a>
 * @author José Bolina
 */
public class HGETALL extends RespCommand implements Resp3Command {

   public HGETALL() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      CompletionStage<Collection<byte[]>> cs = hashMap.get(arguments.get(0))
            .thenApply(res -> {
               if (res == null) return null;

               List<byte[]> keyValues = new ArrayList<>(res.size() * 2);
               for (Map.Entry<byte[], byte[]> entry : res.entrySet()) {
                  keyValues.add(entry.getKey());
                  keyValues.add(entry.getValue());
               }
               return keyValues;
            });
      return handler.stageToReturn(cs, ctx, ByteBufferUtils::bytesToResult);
   }
}
