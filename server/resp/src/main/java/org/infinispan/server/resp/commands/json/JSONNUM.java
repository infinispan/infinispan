package org.infinispan.server.resp.commands.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

abstract class JSONNUM extends RespCommand implements Resp3Command {
    public JSONNUM(String commandName, long aclMask) {
        super(commandName, -2, 1, 1, 1, aclMask);
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        if (arguments.size() < 2) {
            handler.writer().syntaxError();
            return handler.myStage();
        }

        JSONCommandArgumentReader.CommandArgs commandArgs;
        byte[] key;
        byte[] jsonPath;
        byte[] number;
        if (arguments.size() > 2) {
            commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
            key = commandArgs.key();
            jsonPath = commandArgs.jsonPath();
            number = arguments.get(2);
        } else {
            key = arguments.get(0);
            jsonPath = JSONUtil.toJsonPath(JSONCommandArgumentReader.DEFAULT_COMMAND_PATH);
            number = arguments.get(1);
        }
        CompletionStage<List<Number>> result = perform(handler.getJsonCache(), key, jsonPath, number);
        return handler.stageToReturn(result, ctx, JSONNUM::collectionNumbers);
    }

    abstract CompletionStage<List<Number>> perform(EmbeddedJsonCache ejc, byte[] key, byte[] jsonPath, byte[] value);

    private static void collectionNumbers(List<Number> numbers, ResponseWriter writer) {
        if (numbers != null) {
        writer.array(numbers, JSONNUM::singleNumber);
        } else {
            writer.error("-ERR result is not a number");
        }

    }

    private static void singleNumber(Number number, ResponseWriter writer) {
        if (number == null) {
            writer.nulls();
            return;
        }
        if (number instanceof Integer || number instanceof Long || number instanceof BigDecimal || number instanceof BigInteger) {
            writer.integers(number.longValue());
            return;
        }
        if (number instanceof Double || number instanceof Float) {
            writer.doubles(number.doubleValue());
            return;
        }

        writer.string(number.toString());
    }
}
