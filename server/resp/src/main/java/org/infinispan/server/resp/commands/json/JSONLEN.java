package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * Super Class for common code of LEN JSON commands
 *
 * @since 15.2
 */
public abstract class JSONLEN extends RespCommand implements Resp3Command {

    public JSONLEN(String commandName, long aclMask) {
        super(commandName, -2, 1, 1, 1, aclMask);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Long>> lengths = len(ejc, commandArgs.key(), commandArgs.jsonPath());
        if (commandArgs.isLegacy()) {
            return handler.stageToReturn(lengths, ctx, legacyOutput(commandArgs.path()));
        }
        return handler.stageToReturn(lengths, ctx, newArrayOrErrorWriter(commandArgs.path()));
    }

    protected abstract CompletionStage<List<Long>> len(EmbeddedJsonCache ejc, byte[] key, byte[] path);

    BiConsumer<List<Long>, ResponseWriter> newArrayOrErrorWriter(byte[] path) {
        return (c, writer) -> {
            if (c == null) {
                throw new RuntimeException("could not perform this operation on a key that doesn't exist");
            }
            writer.array(c, Resp3Type.INTEGER);
        };
    }

    protected abstract void raiseTypeError(byte[] path);

    BiConsumer<List<Long>, ResponseWriter> legacyOutput(byte[] path) {
        return (c, writer) -> {
            if (c == null || c.isEmpty()) {
                writer.nulls();
            } else if (c.get(0) == null) {
                raiseTypeError(path);
            } else {
                writer.integers(c.get(0));
            }
        };
    }
}
