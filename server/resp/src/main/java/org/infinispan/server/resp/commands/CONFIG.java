package org.infinispan.server.resp.commands;

import static org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/config/">CONFIG</a>
 * @since 14.0
 */
public class CONFIG extends RespCommand implements Resp3Command {
   public CONFIG() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      String getOrSet = new String(arguments.get(0), StandardCharsets.UTF_8);
      String name = new String(arguments.get(1), StandardCharsets.UTF_8);
      byte[] rawName = arguments.get(1);

      if ("GET".equalsIgnoreCase(getOrSet)) {
         if ("appendonly".equalsIgnoreCase(name)) {
            handler.writer().map(Map.of(rawName, "no"), Resp3Type.BULK_STRING, Resp3Type.SIMPLE_STRING);
         } else if (name.indexOf('*') != -1 || name.indexOf('?') != -1) {
            handler.writer().customError("CONFIG blob pattern matching not implemented");
         } else {
            handler.writer().map(Map.of(rawName, EMPTY_BYTE_ARRAY), Resp3Type.BULK_STRING);
         }
      } else if ("SET".equalsIgnoreCase(getOrSet)) {
         handler.writer().ok();
      } else {
         handler.writer().customError("CONFIG " + getOrSet + " not implemented");
      }
      return handler.myStage();
   }
}
