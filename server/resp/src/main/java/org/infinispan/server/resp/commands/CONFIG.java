package org.infinispan.server.resp.commands;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
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
      super(-2, 0, 0, 0, AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      String getOrSet = new String(arguments.get(0), StandardCharsets.UTF_8);
      String name = new String(arguments.get(1), StandardCharsets.UTF_8);
      String canonicalName = name.toLowerCase(Locale.ROOT);

      if ("GET".equalsIgnoreCase(getOrSet)) {
         switch (canonicalName) {
            case "appendonly":
               handler.writer().map(Map.of(canonicalName, "no"), Resp3Type.BULK_STRING, Resp3Type.SIMPLE_STRING);
               break;
            case "databases":
               handler.writer().map(Map.of(canonicalName, "16"), Resp3Type.BULK_STRING);
               break;
            default:
               if (name.indexOf('*') != -1 || name.indexOf('?') != -1) {
                  handler.writer().customError("CONFIG blob pattern matching not implemented");
               } else {
                  handler.writer().map(Map.of(), Resp3Type.BULK_STRING);
               }
         }
      } else if ("SET".equalsIgnoreCase(getOrSet)) {
         handler.writer().ok();
      } else {
         handler.writer().customError("CONFIG " + getOrSet + " not implemented");
      }
      return handler.myStage();
   }
}
