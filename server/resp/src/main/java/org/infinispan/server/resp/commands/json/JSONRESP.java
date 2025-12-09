package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.json.JSONCommandArgumentReader.CommandArgs;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.RESP
 *
 * @see <a href="https://redis.io/commands/json.resp">JSON.RESP</a>
 * @since 15.2
 */
public class JSONRESP extends RespCommand implements Resp3Command {

   public JSONRESP() {
      super("JSON.RESP", -2, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<List<Object>> result = ejc.resp(commandArgs.key(), commandArgs.jsonPath());
      return handler.stageToReturn(result, ctx, new JsonRespResponseSerializer(commandArgs));
   }

   private static class JsonRespResponseSerializer implements JavaObjectSerializer<List<Object>> {
      private final boolean isLegacy;
      private final String jsonPath;

      public JsonRespResponseSerializer(CommandArgs commandArgs) {
         this.isLegacy = commandArgs.isLegacy();
         this.jsonPath = RespUtil.utf8(commandArgs.jsonPath());
      }

      @Override
      public void accept(List<Object> list, ResponseWriter writer) {
         if (list == null) {
            writer.nulls();
            return;
         }
         if (this.isLegacy) {
            if (list.isEmpty()) {
               writer.error("-ERR Path '" + this.jsonPath + "' does not exist");
               return;
            }
            writeNode(list.get(0), writer);
            return;
         }
         writeNode(list, writer);
      }

      private void writeNode(Object object, ResponseWriter writer) {
         if (object == null) {
            writer.nulls();
            return;
         }
         if (object instanceof Integer i) {
            writer.integers(i);
            return;
         }
         if (object instanceof Long l) {
            writer.integers(l);
            return;
         }
         if (object instanceof Double d) {
            writer.doubles(d);
            return;
         }
         if (object instanceof Character c) {
            writer.simpleString(c.toString());
            return;
         }
         if (object instanceof String s) {
            writer.string(s);
            return;
         }
         if (object instanceof Boolean b) {
            writer.simpleString(b ? RespConstants.TRUE : RespConstants.FALSE);
            return;
         }
         if (object instanceof List<?> list) {
            writer.writeNumericPrefix(RespConstants.ARRAY, list.size());
            for (Object node : list) {
               writeNode(node, writer);
            }
         }
      }
   }
}
