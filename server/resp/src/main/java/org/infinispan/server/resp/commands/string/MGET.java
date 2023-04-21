package org.infinispan.server.resp.commands.string;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @link https://redis.io/commands/mget/
 * @since 14.0
 */
public class MGET extends RespCommand implements Resp3Command {
   public MGET() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int keysToRetrieve = arguments.size();
      if (keysToRetrieve == 0) {
         ByteBufferUtils.stringToByteBuf("*0\r\n", handler.allocatorToUse());
         return handler.myStage();
      }
      List<byte[]> results = Collections.synchronizedList(Arrays.asList(
            new byte[keysToRetrieve][]));
      AtomicInteger resultBytesSize = new AtomicInteger();
      AggregateCompletionStage<Void> getStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < keysToRetrieve; ++i) {
         int innerCount = i;
         byte[] keyBytes = arguments.get(i);
         getStage.dependsOn(handler.cache().getAsync(keyBytes)
               .whenComplete((returnValue, t) -> {
                  if (returnValue != null) {
                     results.set(innerCount, returnValue);
                     int length = returnValue.length;
                     if (length > 0) {
                        // $ + digit length (log10 + 1) + /r/n + byte length
                        resultBytesSize.addAndGet(1 + (int) Math.log10(length) + 1 + 2 + returnValue.length);
                     } else {
                        // $0 + /r/n
                        resultBytesSize.addAndGet(2 + 2);
                     }
                  } else {
                     // $-1
                     resultBytesSize.addAndGet(3);
                  }
                  // /r/n
                  resultBytesSize.addAndGet(2);
               }));
      }
      return handler.stageToReturn(getStage.freeze(), ctx, (ignore, alloc) -> {
         int elements = results.size();
         // * + digit length (log10 + 1) + \r\n + accumulated bytes
         int byteAmount = 1 + (int) Math.log10(elements) + 1 + 2 + resultBytesSize.get();
         ByteBuf byteBuf = alloc.apply(byteAmount);
         byteBuf.writeCharSequence("*" + results.size(), CharsetUtil.US_ASCII);
         byteBuf.writeByte('\r');
         byteBuf.writeByte('\n');
         for (byte[] value : results) {
            if (value == null) {
               byteBuf.writeCharSequence("$-1", CharsetUtil.US_ASCII);
            } else {
               byteBuf.writeCharSequence("$" + value.length, CharsetUtil.US_ASCII);
               byteBuf.writeByte('\r');
               byteBuf.writeByte('\n');
               byteBuf.writeBytes(value);
            }
            byteBuf.writeByte('\r');
            byteBuf.writeByte('\n');
         }
         assert byteBuf.writerIndex() == byteAmount;
      });
   }
}
