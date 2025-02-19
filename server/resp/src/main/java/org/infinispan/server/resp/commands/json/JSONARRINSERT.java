package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * JSON.ARRINSERT
 *
 * @see <a href="https://redis.io/commands/json.arrinsert/">JSON.ARRINSERT</a>
 * @since 15.2
 */
public class JSONARRINSERT extends RespCommand implements Resp3Command {
    public JSONARRINSERT() {
        super("JSON.ARRINSERT", -5, 1, 1, 1);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        int index = Integer.parseInt(new String(arguments.get(2)));
        List<byte[]> values = arguments.subList(3, arguments.size());
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Integer>> lengths = ejc.arrInsert(commandArgs.key(), commandArgs.jsonPath(), index,
                values);
        if (commandArgs.isLegacy()) {
            return handler.stageToReturn(lengths, ctx, newIntegerOrErrorWriter(commandArgs.jsonPath()));
        }
        return handler.stageToReturn(lengths, ctx, JSONARRINSERT::jsonPathWriter);
    }

    private static BiConsumer<List<Integer>, ResponseWriter> newIntegerOrErrorWriter(byte[] jsonPath) {
        return (l, writer) -> {
            if (l == null) {
                writer.error("-ERR could not perform this operation on a key that doesn't exist");
            } else {
                // Returning last non null value
                for (int i = l.size()-1; i>=0; i--)
                    if (l.get(i) != null) {
                        writer.integers(l.get(i));
                        return;
                    }
                }
                writer.error("-ERR Path '" + new String(jsonPath) + "' does not exist or not an array");
        };
    }

    private static void jsonPathWriter(List<Integer> l, ResponseWriter writer) {
        if (l == null) {
            writer.error("-ERR could not perform this operation on a key that doesn't exist");
        } else {
            ResponseWriter.ARRAY_INTEGER.accept(l, writer);
        }
    }

    @Override
    public long aclMask() {
        return 0;
    }

}
