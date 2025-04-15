package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.GET
 *
 * @see <a href="https://redis.io/commands/json.get/">JSON.GET</a>
 * @since 15.2
 */
public class JSONGET extends RespCommand implements Resp3Command {

   public JSONGET() {
      super("JSON.GET", -2, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
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
      CompletionStage<byte[]> result = ejc.get(key, paths, args.space(), args.newline(), args.indent());
      return handler.stageToReturn(result, ctx, ResponseWriter.BULK_STRING_BYTES);
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
