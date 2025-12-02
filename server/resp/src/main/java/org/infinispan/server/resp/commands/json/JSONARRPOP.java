package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.ARRPOP
 *
 * @see <a href="https://redis.io/commands/json.arrpop/">JSON.ARRPOP</a>
 * @since 15.2
 */
public class JSONARRPOP extends RespCommand implements Resp3Command {

    public JSONARRPOP() {
        super("JSON.ARRPOP", -1, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        var cmdArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        var index = (arguments.size() > 2) ? ArgumentUtils.toInt(arguments.get(2)) : -1;
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<byte[]>> result = ejc.arrpop(cmdArgs.key(), cmdArgs.jsonPath(), index);
        return (cmdArgs.isLegacy()) ? handler.stageToReturn(result, ctx, legacyReturn(cmdArgs.jsonPath()))
                : handler.stageToReturn(result, ctx, JSONARRPOP::jsonPathReturn);
    }

    static BiConsumer<List<byte[]>, ResponseWriter> legacyReturn(byte[] path) {
       // legacy path reyturn one non null result, last one in the array
       return (c, writer) -> {
          if (c == null) {
             writer.error("-ERR could not perform this operation on a key that doesn't exist");
             return;
          }
          if (!c.isEmpty()) {
             for (int i = c.size() - 1; i >= 0; i--) {
                if (c.get(i) != null) {
                   // Missing node case
                   if (c.get(i).length == 0) {
                      writer.nulls();
                   } else {
                      writer.string(c.get(i));
                   }
                   return;
                }
             }
          }
          writer.error("-ERR Path '" + RespUtil.utf8(path) + "' does not exist or not an array");
       };
    }

    static void jsonPathReturn(List<byte[]> c, ResponseWriter writer) {
        if (c == null) {
            writer.error("-ERR could not perform this operation on a key that doesn't exist");
        } else {
            writer.array(c, Resp3Type.BULK_STRING_OR_MISSING);
        }
    }
}
