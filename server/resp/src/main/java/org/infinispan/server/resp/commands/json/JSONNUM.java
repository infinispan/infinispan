package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.NumOpType;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletionStage;

public abstract class JSONNUM extends RespCommand implements Resp3Command {
    public JSONNUM(String commandName) {
        super(commandName, -3, 1, 1, 1);
    }

    @Override
    public long aclMask() {
        return 0;
    }

    @Override
    public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                       List<byte[]> arguments) {
        JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
        final byte[] value = arguments.get(2);
        EmbeddedJsonCache ejc = handler.getJsonCache();
        CompletionStage<List<Number>> incrementedValuesCS = ejc.numOp(commandArgs.key(), commandArgs.jsonPath(), value, NumOpType.fromCommand(this.getName()));
        return handler.stageToReturn(incrementedValuesCS, ctx, JSONNUM::collectionNumbers);
    }

    private static void collectionNumbers(List<Number> numbers, ResponseWriter writer) {
        writer.array(numbers,(number, writer2) -> {
            singleNumber(number, writer2);
        });
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
