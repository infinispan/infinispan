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
 * JSON.GET
 *
 * @see <a href="https://redis.io/commands/json.get/">JSON.GET</a>
 * @since 15.1
 */
public class JSONGET extends RespCommand implements Resp3Command {
   // Configure JsonPath to use Jackson. Path with missing final leaf will return
   // null
   // path with more the 1 level missing will rise exception
   static Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public JSONGET() {
      super("JSON.GET", -2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      Args args;
      try {
         args = parseArgs(arguments);
      } catch (Exception ex) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }
      String[] paths = arguments.stream().skip(args.pos()).map(i -> new String(i, StandardCharsets.UTF_8))
            .toArray(String[]::new);
      EmbeddedJsonCache ejc = handler.getJsonDocCache();
      CompletionStage<String> result = ejc.get(key, paths, args.space(), args.newline(), args.indent());
      CompletionStage<String> cs = result;
      return handler.stageToReturn(cs, ctx, ResponseWriter.SIMPLE_STRING);
   }

   /*
    * Returns in a record: indent, newline, space and the pos of the
    * next argument to process
    */
   private Args parseArgs(List<byte[]> arguments) {
      String indent = "";
      String newline = "";
      String space = "";
      int pos = 1;
      while (pos < arguments.size()) {
         switch ((new String(arguments.get(pos))).toUpperCase()) {
            case "INDENT":
               indent = new String(arguments.get(++pos));
               ++pos;
               break;
            case "NEWLINE":
               newline = new String(arguments.get(++pos));
               ++pos;
               break;
            case "SPACE":
               space = new String(arguments.get(++pos));
               ++pos;
               break;
            default:
               return new Args(indent, newline, space, pos);
         }
      }
      return new Args(indent, newline, space, pos);
   }

}
