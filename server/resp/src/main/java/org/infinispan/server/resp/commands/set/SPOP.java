package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SPOP
 *
 * @see <a href="https://redis.io/commands/spop/">SPOP</a>
 * @since 15.0
 */
public class SPOP extends RespCommand implements Resp3Command {
   public SPOP() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.SET.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      final long count = (arguments.size() <= 1) ? 1 : ArgumentUtils.toLong(arguments.get(1));
      if (count < 0) {
         handler.writer().mustBePositive();
         return handler.myStage();
      }
      final byte[] key = arguments.get(0);
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      boolean hasCount = arguments.size() > 1;
      return handler.stageToReturn(esc.pop(key, count, true), ctx,
            hasCount ? ResponseWriter.ARRAY_BULK_STRING : ResponseWriter.FIRST_BULK_STRING_OR_NULL);
   }
}
