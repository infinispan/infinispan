package org.infinispan.server.resp.commands.connection;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.SwitchDbOperation;

import io.netty.channel.ChannelHandlerContext;

/**
 * SELECT
 *
 * @see <a href="https://redis.io/commands/select/">SELECT</a>
 * @since 14.0
 */
public class SELECT extends RespCommand implements Resp3Command {
   public SELECT() {
      super(2, 0, 0, 0, AclCategory.FAST.mask() | AclCategory.CONNECTION.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String db = new String(arguments.get(0), StandardCharsets.US_ASCII);
      try {
         SwitchDbOperation.switchDB(handler, db, ctx);
         handler.writer().ok();
      } catch (CacheException e) {
         handler.writer().customError("DB index is out of range");
      }
      return handler.myStage();
   }
}
