package org.infinispan.server.resp.commands.json;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.SET
 *
 * @see <a href="https://redis.io/commands/json.set/">JSON.SET</a>
 *
 *      Note: this version allows to append an element at the end of an array
 *      while Redis implementation returns error
 * @since 15.2
 */
public class JSONSET extends RespCommand implements Resp3Command {

   private static final String XX = "XX";
   private static final String NX = "NX";
   private static final BiConsumer<? super String, ResponseWriter> biConsumer = JSONSET::jsonSetBiConsumer;

   public JSONSET() {
      super("JSON.SET", -4, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] path = arguments.get(1);
      byte[] value = arguments.get(2);
      if (arguments.size() > 4) {
         handler.writer().syntaxError();
         return handler.myStage();
      }
      boolean nx = false, xx = false;
      if (arguments.size() == 4) {
         String arg = (new String(arguments.get(3), StandardCharsets.UTF_8).toUpperCase());
         switch (arg) {
            case NX:
               nx = true;
               break;
            case XX:
               xx = true;
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      }
      EmbeddedJsonCache ejc = handler.getJsonCache();
      if (JSONUtil.isValueInvalid(value)) {
         handler.writer().customError("Invalid json value for JSON.SET");
         return handler.myStage();
      }
      CompletionStage<String> cs = ejc.set(key, value, path, nx, xx);
      return handler.stageToReturn(cs, ctx, biConsumer);
   }

   private static void jsonSetBiConsumer(String value, ResponseWriter writer) {
      if (value == null) {
         // Use null consumer
         writer.nulls();
         return;
      }
      if ("OK".equals(value)) {
         // Use ok consumer
         writer.ok();
         return;
      }
      // All the rest are errors
      writer.error(value);
   }

}
