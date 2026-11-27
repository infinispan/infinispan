package org.infinispan.server.resp.commands.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.stream.StreamID;

import io.netty.channel.ChannelHandlerContext;

/**
 * XDEL
 *
 * @see <a href="https://redis.io/commands/xdel/">XDEL</a>
 * @since 16.4
 */
public class XDEL extends RespCommand implements Resp3Command {

   public XDEL() {
      super(-3, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.STREAM.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<StreamID> ids = new ArrayList<>(arguments.size() - 1);
      for (int i = 1; i < arguments.size(); i++) {
         try {
            ids.add(StreamID.parse(arguments.get(i)));
         } catch (NumberFormatException e) {
            throw new RespCommandException("ERR Invalid stream ID specified as stream command argument");
         }
      }
      return handler.stageToReturn(handler.getStreamCache().delete(key, ids), ctx, ResponseWriter.INTEGER);
   }
}
