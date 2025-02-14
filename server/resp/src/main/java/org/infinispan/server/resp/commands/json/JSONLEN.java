package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.LenType;
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

    private LenType lenType;
    private boolean includePathOnError;

    public JSONLEN(String commandName) {
        this(commandName, false);
    }

    public JSONLEN(String commandName, boolean includePathOnError) {
        super(commandName, -2, 1, 1, 1);
        this.lenType = LenType.fromCommand(commandName);
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
        CompletionStage<List<Long>> lengths = ejc.len(commandArgs.key(), commandArgs.jsonPath(), lenType);
        if (commandArgs.isLegacy()) {
            return handler.stageToReturn(lengths, ctx, JSONLEN::integerOrNullWriter);
        }
        return handler.stageToReturn(lengths, ctx, newArrayOrErrorWriter(commandArgs.jsonPath()));
    }

    BiConsumer<List<Long>, ResponseWriter> newArrayOrErrorWriter(byte[] path) {
        return (c, writer) -> {
            if (c == null || c.size() == 0) {
                if (includePathOnError) {
                    throw new RuntimeException("Path '" + RespUtil.ascii(path) + "' does not exist or not an object");
                }
                throw new RuntimeException("could not perform this operation on a key that doesn't exist");
            }
            writer.array(c, Resp3Type.INTEGER);
        };
    }

    static void integerOrNullWriter(List<Long> c, ResponseWriter w) {
        if (c == null || c.size() == 0) {
            w.nulls();
        } else {
            w.integers(c.get(0));
        }
    }
}
