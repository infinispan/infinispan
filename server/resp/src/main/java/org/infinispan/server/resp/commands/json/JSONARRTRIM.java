package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.ARRTRIM
 *
 * @see <a href="https://redis.io/commands/json.arrtrim/">JSON.ARRTRIM</a>
 * @since 15.2
 */
public class JSONARRTRIM extends RespCommand implements Resp3Command {
    public static String ARR_TYPE_NAME = "array";

    public JSONARRTRIM() {
        super("JSON.ARRTRIM", 5, 1, 1, 1);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        int start = Integer.parseInt(new String(arguments.get(2)));
        int stop = Integer.parseInt(new String(arguments.get(3)));
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Integer>> lengths = ejc.arrTrim(commandArgs.key(), commandArgs.jsonPath(), start, stop);
        if (commandArgs.isLegacy()) {
            return handler.stageToReturn(lengths, ctx, legacyReturn(commandArgs.jsonPath()));
        }
        return handler.stageToReturn(lengths, ctx, JSONARRTRIM::jsonPathReturn);

    }

    static BiConsumer<List<Integer>, ResponseWriter> legacyReturn(byte[] path) {
        // legacy path just one result and it must be not null
        return (c, writer) -> {
            if (c == null) {
                writer.error("-ERR could not perform this operation on a key that doesn't exist");
            } else {
                // For compatibility, last non-null result is returned
                for (int i = c.size() - 1; i >= 0; i--) {
                    if (c.get(i) != null) {
                        writer.integers(c.get(i));
                        return;
                    }
                }
                writer.error("-ERR Path '" + RespUtil.utf8(path) + "' does not exist or not an array");
            }
        };
    }

    static void jsonPathReturn(List<Integer> c, ResponseWriter writer) {
        if (c == null) {
            writer.error("-ERR could not perform this operation on a key that doesn't exist");
        } else {
            writer.array(c, Resp3Type.INTEGER);
        }
    }

    @Override
    public long aclMask() {
        return 0;
    }

}
