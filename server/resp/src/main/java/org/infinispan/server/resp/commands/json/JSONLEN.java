package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

/**
 * Super Class for common code of LEN JSON commands
 *
 * @since 15.2
 */
public abstract class JSONLEN extends RespCommand implements Resp3Command {

    private boolean includePathOnError;

    public JSONLEN(String commandName) {
        this(commandName, false);
    }

    public JSONLEN(String commandName, boolean includePathOnError) {
        super(commandName, -2, 1, 1, 1);
        this.includePathOnError = includePathOnError;
    }

    @Override
    public long aclMask() {
        return 0;
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
                if (includePathOnError) {
                    raiseTypeError(path);
                }
                throw new RuntimeException("could not perform this operation on a key that doesn't exist");
            }
            writer.array(c, Resp3Type.INTEGER);
        };
    }

    protected abstract void raiseTypeError(byte[] path);

    BiConsumer<List<Long>, ResponseWriter> legacyOutput(byte[] path) {
        return (c, writer) -> {
            if (c == null) {
                writer.nulls();
            } else if (c.isEmpty()) {
                throw new RuntimeException("Path '" + RespUtil.ascii(path) + "' does not exist");
            } else if (c.get(0) == null) {
                raiseTypeError(path);
            } else {
                writer.integers(c.get(0));
            }
        };
    }
}
