package org.infinispan.server.resp.commands.list;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LINDEX
 *
 * @see <a href="https://redis.io/commands/lindex/">LINDEX</a>
 * @since 15.0
 */
public class LINDEX extends RespCommand implements Resp3Command {
   public LINDEX() {
      super(3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      final long index = Long.parseLong(new String(arguments.get(1), StandardCharsets.US_ASCII));

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<byte[]> value = listMultimap.index(key, index);
      return handler.stageToReturn(value, ctx, ResponseWriter.BULK_STRING_BYTES);
   }
}
