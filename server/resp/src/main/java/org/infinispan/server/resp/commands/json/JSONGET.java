package org.infinispan.server.resp.commands.json;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.GET
 *
 * @see <a href="https://redis.io/commands/json.get/">JSON.GET</a>
 * @since 15.1
 */
public class JSONGET extends RespCommand implements Resp3Command {
   private String indent;
   private String newline;
   private String space;

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
      int pos = 0;
      byte[] key = arguments.get(pos++);
      try {
         pos = parseArgs(arguments, pos);
      } catch (Exception ex) {
         handler.writer().wrongArgumentNumber(this);
      }

      FunctionalMap.ReadOnlyMap<byte[], Object> cache = ReadOnlyMapImpl
            .create(FunctionalMapImpl.create(handler.typedCache(null)));
      int finalPos = pos;
      CompletionStage<String> cs = cache.eval(key, view -> {
         int pathPos = finalPos;
         ObjectMapper mapper = new ObjectMapper();
         var doc = (byte[]) view.find().orElse(null);
         try {
            var rootNode = mapper.readTree(doc);
            var jpCtx = JsonPath.using(config).parse(rootNode);
            if (pathPos == arguments.size()-1) {
               return rootNode.toString();
            }
            ObjectNode result = mapper.createObjectNode();
            while (pathPos < arguments.size()) {
               var pathStr = new String(arguments.get(pathPos++));
               JsonNode node = jpCtx.read(pathStr);
               result.set(pathStr, node);
            }
            return result.toString();
         } catch (IOException e) {
            return e.getMessage();
         }
      });
      return handler.stageToReturn(cs, ctx, ResponseWriter.SIMPLE_STRING);
   }

   private int parseArgs(List<byte[]> arguments, int pos) {
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
               return pos;
         }
      }
      return pos;
   }

}
