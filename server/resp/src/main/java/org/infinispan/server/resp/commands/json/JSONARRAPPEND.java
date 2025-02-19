package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.json.EmbeddedJsonCache;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * JSON.ARRAPPEND
 *
 * @see <a href="https://redis.io/commands/json.arrappend/">JSON.ARRAPPEND</a>
 * @since 15.2
 */
public class JSONARRAPPEND extends JSONAPPEND {
    public static String ARR_TYPE_NAME = "array";
    public JSONARRAPPEND() {
        super("JSON.ARRAPPEND", -4, 1, 1, 1);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        List<byte[]> values = arguments.subList(2, arguments.size());
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Long>> lengths = ejc.arrAppend(commandArgs.key(), commandArgs.jsonPath(), values);
        return returnResult(handler, ctx, commandArgs.jsonPath(), commandArgs.isLegacy(), lengths);
    }

    @Override
    protected String getOpType() {
        return ARR_TYPE_NAME;
    }

}
