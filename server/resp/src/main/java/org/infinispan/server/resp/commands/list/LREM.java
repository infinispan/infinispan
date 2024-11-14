package org.infinispan.server.resp.commands.list;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * LREM
 *
 * @see <a href="https://redis.io/commands/lrem/">LREM</a>
 * @since 15.0
 */
public class LREM extends RespCommand implements Resp3Command {

   public LREM() {
      super(4, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.LIST | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      Long count;
      try {
         count = ArgumentUtils.toLong(arguments.get(1));
      } catch (NumberFormatException e) {
         handler.writer().customError("value is not an integer or out of range");
         return handler.myStage();
      }

      byte[] element = arguments.get(2);

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      return handler.stageToReturn(listMultimap.remove(key, count, element), ctx, ResponseWriter.INTEGER);
   }
}
