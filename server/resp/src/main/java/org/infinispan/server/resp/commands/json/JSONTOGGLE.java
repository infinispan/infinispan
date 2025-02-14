package org.infinispan.server.resp.commands.json;

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
 * JSON.TOGGLE
 *
 * @see <a href="https://redis.io/commands/json.toggle/">JSON.TOGGLE</a>
 * @since 15.2
 */
public class JSONTOGGLE extends RespCommand implements Resp3Command {

    public JSONTOGGLE() {
        super("JSON.TOGGLE", -2, 1, 1, 1);
    }

    @Override
    public long aclMask() {
        return 0;
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        final byte[] key = arguments.get(0);
        final byte[] path = arguments.get(1);
        final byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean isLegacy = path != jsonPath;
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Integer>> lengths = ejc.toggle(key, jsonPath);

        if (isLegacy) {
            return handler.stageToReturn(lengths.thenApply(l -> {
                if (l.isEmpty()) {
                    throw new RuntimeException(String.format("Path '%s' does not exist or not a bool", new String(jsonPath)));
                }
                return l.get(0) == 0 ? "false" : "true";
            }), ctx, ResponseWriter.SIMPLE_STRING);
        }
        return handler.stageToReturn(lengths, ctx, ResponseWriter.ARRAY_INTEGER);
    }
}
