package org.infinispan.server.resp.commands.hash;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * HKEYS
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hkeys/">HKEYS</a>
 * @since 15.0
 */
public class HKEYS extends RespCommand implements Resp3Command {

   public HKEYS() {
      super(2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.HASH.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> multimap = handler.getHashMapMultimap();
      CompletionStage<Collection<byte[]>> cs = multimap.keySet(arguments.get(0))
            .thenApply(Collections::unmodifiableCollection);
      return handler.stageToReturn(cs, ctx, ResponseWriter.ARRAY_BULK_STRING);
   }
}
