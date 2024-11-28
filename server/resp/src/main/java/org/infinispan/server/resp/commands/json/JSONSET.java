package org.infinispan.server.resp.commands.json;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.SET
 *
 * @see <a href="https://redis.io/commands/json.set/">JSON.SET</a>
 * @since 15.1
 */
public class JSONSET extends RespCommand implements Resp3Command {

   public static final String XX = "XX";
   public static final String NX = "NX";
   // Configure JsonPath to use Jackson. Path with missing final leaf will return
   // null
   // path with more the 1 level missing will rise exception
   static Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public static void jsonSetBiConsumer(String value, ResponseWriter writer) {
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

   public JSONSET() {
      super("JSON.SET", -3, 1, 1, 1);
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
      boolean nx=false, xx=false;
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
      EmbeddedJsonCache ejc = handler.getJsonDocCache();
      CompletionStage<String> result = ejc.set(key, new String(value, StandardCharsets.UTF_8),
            new String(path, StandardCharsets.UTF_8), nx, xx);
      CompletionStage<String> cs = result;
      return handler.stageToReturn(cs, ctx, JSONSET::jsonSetBiConsumer);
   }
}
