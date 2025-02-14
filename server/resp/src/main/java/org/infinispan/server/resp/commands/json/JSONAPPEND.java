package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * superclass for JSON.STRAPPEND and JSON.ARRAPPEND
 *
 * @since 15.2
 */
public abstract class JSONAPPEND extends RespCommand implements Resp3Command {

    protected JSONAPPEND(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
        super(name, arity, firstKeyPos, lastKeyPos, steps);
    }

    protected CompletionStage<RespRequestHandler> returnResult(Resp3Handler handler, ChannelHandlerContext ctx,
                                                               byte[] jsonPath, boolean isLegacy,
                                                               CompletionStage<List<Long>> lengths) {
        if (isLegacy) {
            return handler.stageToReturn(lengths, ctx, newIntegerOrErrorWriter(jsonPath, getOpType()));
        }

        return handler.stageToReturn(lengths, ctx, ResponseWriter.ARRAY_INTEGER);
    }

    @Override
    public long aclMask() {
        return 0;
    }

    static BiConsumer<List<Long>, ResponseWriter> newIntegerOrErrorWriter(byte[] path, String opType) {
        // legacy path just one result and it must be not null
        return (c, writer) -> {
            if (c == null || c.size() == 0 || c.get(0) == null) {
                throw new CacheException("Path '" + RespUtil.ascii(path) + "' does not exist or not a " + opType);
            }
            // For compatibility, last non-null result is returned
            for (int i = c.size() - 1; i >= 0; i--) {
                if (c.get(i) != null) {
                    writer.integers(c.get(i));
                    return;
                }
            }
        };
    }

    protected abstract String getOpType();
}
