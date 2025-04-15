package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.STRAPPEND
 *
 * @see <a href="https://redis.io/commands/json.strappend/">JSON.STRAPPEND</a>
 * @since 15.2
 */
public class JSONSTRAPPEND extends JSONAPPEND {
    public static String STR_TYPE_NAME = "string";
    public JSONSTRAPPEND() {
        super("JSON.STRAPPEND", -3, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {

        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        byte[] value = arguments.get(1);
        byte[] jsonPath;
        if (arguments.size() > 2) {
            jsonPath = commandArgs.jsonPath();
            value = arguments.get(2);
        } else {
            jsonPath = JSONUtil.toJsonPath(JSONCommandArgumentReader.DEFAULT_COMMAND_PATH);
        }

        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Long>> lengths = ejc.strAppend(commandArgs.key(), jsonPath, value);
        return returnResult(handler, ctx, jsonPath, commandArgs.isLegacy(), lengths);
    }

    @Override
    protected String getOpType() {
        return STR_TYPE_NAME;
    }

}
