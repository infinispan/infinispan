package org.infinispan.server.resp.commands;

import static org.infinispan.commons.util.Util.EMPTY_BYTE_ARRAY;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/config/">Redis documentation</a>
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
            Resp3Response.map(Map.of(rawName, "no"), handler.allocator(), Resp3Type.BULK_STRING, Resp3Type.SIMPLE_STRING);
         } else if (name.indexOf('*') != -1 || name.indexOf('?') != -1) {
            RespErrorUtil.customError("CONFIG blob pattern matching not implemented", handler.allocator());
         } else {
            Resp3Response.map(Map.of(rawName, EMPTY_BYTE_ARRAY), handler.allocator(), Resp3Type.BULK_STRING);
         }
      } else if ("SET".equalsIgnoreCase(getOrSet)) {
         Resp3Response.ok(handler.allocator());
      } else {
         RespErrorUtil.customError("CONFIG " + getOrSet + " not implemented", handler.allocator());
      }
      return handler.myStage();
   }
}
