package org.infinispan.server.resp.commands.hash;

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
 * HGET
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hget/">HGET</a>
 * @since 15.0
 */
public class HGET extends RespCommand implements Resp3Command {

   public HGET() {
      super(3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.HASH.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      return handler.stageToReturn(hashMap.get(arguments.get(0), arguments.get(1)), ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
