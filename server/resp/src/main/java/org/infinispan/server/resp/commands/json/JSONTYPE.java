package org.infinispan.server.resp.commands.json;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.jayway.jsonpath.DocumentContext;
import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * JSON.TYPE
 *
 * @see <a href="https://redis.io/commands/json.type/">JSON.TYPE</a>
 * @since 15.2
 */
public class JSONTYPE extends RespCommand implements Resp3Command {

   public JSONTYPE() {
      super("JSON.TYPE", -2, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      Args args;
      args = parseArgs(arguments);
      if (args == null) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }
      List<byte[]> paths = arguments.subList(args.pos, arguments.size());
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<String> result = ejc.get(key, paths, args.space(), args.newline(), args.indent())
              .thenApply(json -> {
                 if (json == null) {
                    return null;
                 }
                 DocumentContext parse = JSONUtil.parserForGet.parse(json);

                 return "object";
              });

//      "json": If the root key contains a JSON object.
//      "array": If the value is a JSON array.
//              "string": If the value is a string.
//      "number": If the value is a number.
//      "boolean": If the value is a boolean (true or false).
//      "null": If the value is null.

      return handler.stageToReturn(result, ctx, ResponseWriter.BULK_STRING);
   }

   /*
    * Returns in a record: indent, newline, space and the pos of the
    * next argument to process
    */
   private Args parseArgs(List<byte[]> arguments) {
      byte[] indent = null;
      byte[] newline = null;
      byte[] space = null;
      int pos = 1;
      while (pos < arguments.size()) {
         switch ((new String(arguments.get(pos))).toUpperCase()) {
            case "INDENT":
               if (++pos >= arguments.size()) {
                  return null;
               }
               indent = arguments.get(pos++);
               break;
            case "NEWLINE":
               if (++pos >= arguments.size()) {
                  return null;
               }
               newline = arguments.get(pos++);
               break;
            case "SPACE":
               if (++pos >= arguments.size()) {
                  return null;
               }
               space = arguments.get(pos++);
               break;
            default:
               return new Args(indent, newline, space, pos);
         }
      }
      return new Args(indent, newline, space, pos);
   }

   record Args(byte[] indent, byte[] newline, byte[] space, int pos) {
   }
}
