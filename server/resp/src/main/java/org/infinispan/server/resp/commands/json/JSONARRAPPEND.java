package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.json.AppendType;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.ARRAPPEND
 *
 * @see <a href="https://redis.io/commands/json.arrappend/">JSON.ARRAPPEND</a>
 * @since 15.2
 */
public class JSONARRAPPEND extends JSONAPPEND {
    public static String ARR_TYPE_NAME = AppendType.ARRAY.name().toLowerCase();
    public JSONARRAPPEND() {
        super("JSON.ARRAPPEND", -3, 1, 1, 1);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        byte[] key = arguments.get(0);
        byte[] path = arguments.get(1);
        List<byte[]> values = arguments.subList(2, arguments.size());
        // To keep compatibility, considering the first path only. Additional args will
        // be ignored
        // If missing, default path '.' is used, it's in legacy style, i.e. not jsonpath
        byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean withPath = path == jsonPath;
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Long>> lengths = ejc.arrAppend(key, jsonPath, values);
        return returnResult(handler, ctx, jsonPath, withPath, lengths);
    }

    @Override
    protected String getOpType() {
        return ARR_TYPE_NAME;
    }

}
