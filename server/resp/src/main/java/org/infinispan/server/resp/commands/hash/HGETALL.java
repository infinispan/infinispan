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
 * HGETALL
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/hgetall/">HGETALL</a>
 * @since 15.0
 */
public class HGETALL extends RespCommand implements Resp3Command {

   public HGETALL() {
      super(2, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.READ | AclCategory.HASH | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      EmbeddedMultimapPairCache<byte[], byte[], byte[]> hashMap = handler.getHashMapMultimap();
      return handler.stageToReturn(hashMap.get(arguments.get(0)), ctx, ResponseWriter.MAP_BULK_STRING_KV);
   }
}
