package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.ARRINDEX
 *
 * @see <a href="https://redis.io/commands/json.arrindex/">JSON.ARRINDEX</a>
 * @since 15.2
 */
public class JSONARRINDEX extends RespCommand implements Resp3Command {

    public JSONARRINDEX() {
        super("JSON.ARRINDEX", -4, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        byte[] key = arguments.get(0);
        byte[] path = arguments.get(1);
        byte[] value = arguments.get(2);
        int start = 0;
        if (arguments.size() >= 4) {
            start = ArgumentUtils.toInt(arguments.get(3));
        }
        int stop = 0;
        if (arguments.size() >= 5) {
            stop = ArgumentUtils.toInt(arguments.get(4));
        }
        byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean isLegacy = path != jsonPath;
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Integer>> result = ejc.arrIndex(key, jsonPath, value, start, stop, isLegacy);
        return (isLegacy) ? handler.stageToReturn(result.thenApply(l -> l.get(0)), ctx, ResponseWriter.INTEGER)
                : handler.stageToReturn(result, ctx, ResponseWriter.ARRAY_INTEGER);
    }
}
