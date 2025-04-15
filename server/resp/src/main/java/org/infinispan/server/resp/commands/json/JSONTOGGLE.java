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
 * JSON.TOGGLE
 *
 * @see <a href="https://redis.io/commands/json.toggle/">JSON.TOGGLE</a>
 * @since 15.2
 */
public class JSONTOGGLE extends RespCommand implements Resp3Command {

    private static final String FALSE = Boolean.toString(false);
    private static final String TRUE = Boolean.toString(true);

    public JSONTOGGLE() {
        super("JSON.TOGGLE", -2, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Integer>> lengths = ejc.toggle(commandArgs.key(), commandArgs.jsonPath());

        if (commandArgs.isLegacy()) {
            return handler.stageToReturn(lengths.thenApply(l -> {
                if (l.isEmpty()) {
                    throw new RuntimeException(String.format("Path '%s' does not exist or not a bool", new String(commandArgs.jsonPath())));
                }
                return l.get(0) == 0 ? FALSE : TRUE;
            }), ctx, ResponseWriter.SIMPLE_STRING);
        }
        return handler.stageToReturn(lengths, ctx, ResponseWriter.ARRAY_INTEGER);
    }
}
