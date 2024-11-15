package org.infinispan.server.resp.commands.json;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

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

   public static void jsonSetBiConsumer(CharSequence value, ResponseWriter writer) {
      if (value == null) {
         // Use null consumer
         writer.nulls();
         return;
      }
      if (value.length() == 2 && value.charAt(0) == 'O' && value.charAt(1) == 'K') {
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
      boolean nx, xx; // must be effectively final
      if (arguments.size() == 4) {
         String arg = (new String(arguments.get(3)).toUpperCase());
         switch (arg) {
            case NX:
               nx = true;
               xx = false;
               break;
            case XX:
               xx = true;
               nx = false;
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      } else {
         nx = false;
         xx = false;
      }
      JsonNode newNode;
      try {
         newNode = JSONUtil.objectMapper.readTree(value);
      } catch (IOException e) {
         throw new CacheException(e);
      }
      FunctionalMap.ReadWriteMap<byte[], Object> cache = ReadWriteMapImpl
            .create(FunctionalMapImpl.create(handler.typedCache(null)));
      CompletionStage<CharSequence> cs = cache.eval(key, view -> {
         var doc = (byte[]) view.find().orElse(null);
         if (doc == null) {
            if (xx) {
               return null;
            }
            if (!isRoot(path)) {
               throw new CacheException("new objects must be created at root");
            }
            view.set(value);
            return RespConstants.OK;
         }
         if (nx) {
            return null;
         }
         if (isRoot(path)) {
            // Updating the root node is not allowed by jsonpath
            // replacing the whole doc here
            view.set(value);
            return RespConstants.OK;
         }
         try {
            var rootObjectNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc);
            var jpCtx = JsonPath.using(config).parse(rootObjectNode);
            var pathStr = new String(path);
            JsonNode node = jpCtx.read(pathStr);
            if (node.isNull() && xx || !node.isNull() && nx) {
               return null;
            }
            jpCtx.set(pathStr, newNode);
            view.set(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode));
            return RespConstants.OK;
         } catch (PathNotFoundException ex) {
            // mimicking redis. Not an error, do nothing and return null
            return null;
         } catch (Exception e) {
            throw new CacheException(e);
         }
      });
      return handler.stageToReturn(cs, ctx, JSONSET::jsonSetBiConsumer);
   }

   private static boolean isRoot(byte[] path) {
      return path.length == 1 && path[0] == '$';
   }

}
