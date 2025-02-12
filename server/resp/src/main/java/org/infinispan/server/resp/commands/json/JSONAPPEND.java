package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * superclass for JSON.STRAPPEND and JSON.ARRAPPEND
 * @since 15.2
 */
public abstract class JSONAPPEND extends RespCommand implements Resp3Command {

    protected JSONAPPEND(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
        super(name, arity, firstKeyPos, lastKeyPos, steps);
    }

    @Override
    public long aclMask() {
        return 0;
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {

        byte[] key = arguments.get(0);
        byte[] path = JSONUtil.DEFAULT_PATH;
        byte[] value = arguments.get(1);
        if (arguments.size() > 2) {
            path = arguments.get(1);
            value = arguments.get(2);
        }
        // To keep compatibility, considering the first path only. Additional args will
        // be ignored
        // If missing, default path '.' is used, it's in legacy style, i.e. not jsonpath
        byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean withPath = path == jsonPath;
        CompletionStage<List<Long>> lengths = performAppend(handler, key, jsonPath, value);

        /*
         * Return value depends on some logic: for jsonpath return an array of lengths for all the
         * matching path, empty array is allowed for old legacy path return one length as a Number
         * an error if path doesn't exist or is not of the right type.
         */
        if (withPath) {
            return handler.stageToReturn(lengths, ctx, ResponseWriter.ARRAY_INTEGER);
        }
        return handler.stageToReturn(lengths, ctx, newIntegerOrErrorWriter(jsonPath, getOpType()));
    }

    static BiConsumer<List<Long>, ResponseWriter> newIntegerOrErrorWriter(byte[] path,String opType) {
        // legacy path just one result and it must be not null
        return (c, writer) -> {
            if (c == null || c.size() == 0 || c.get(0) == null) {
                throw new RuntimeException(
                        "Path '" + RespUtil.ascii(path) + "' does not exist or not a " + opType);
            }
            writer.integers(c.get(0));
        };
    }

    protected abstract String getOpType();

    protected abstract CompletionStage<List<Long>> performAppend(Resp3Handler handler, byte[] key, byte[] jsonPath,
                                                                 byte[] value);
}
