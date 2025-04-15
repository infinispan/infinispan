package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SMEMBERS
 *
 * @see <a href="https://redis.io/commands/smembers/">SMEMBERS</a>
 * @since 15.0
 */
public class SMEMBERS extends RespCommand implements Resp3Command {
   public SMEMBERS() {
      super(2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      return handler.stageToReturn(esc.getAsSet(arguments.get(0)), ctx, ResponseWriter.SET_BULK_STRING);
   }
}
