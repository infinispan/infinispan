package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

/**
 * SETRANGE Resp Command
 *
 * @link https://redis.io/commands/setrange/
 * @since 15.0
 */
public class SETRANGE extends RespCommand implements Resp3Command {
   public SETRANGE() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);
      int offset = ArgumentUtils.toInt(arguments.get(1));
      byte[] patch = arguments.get(2);
      CompletionStage<Long> objectCompletableFuture = handler.cache().computeAsync(keyBytes, (ignoredKey, value) -> setrange(value, patch, offset)).thenApply(newValue -> (long)newValue.length);
      return handler.stageToReturn(objectCompletableFuture, ctx, Consumers.LONG_BICONSUMER);
   }

   private byte[] setrange(byte[] value, byte[] patch, int offset) {
      if (value==null) {
         value = Util.EMPTY_BYTE_ARRAY;
      }
      if (patch.length+offset < value.length) {
         var buf = Unpooled.wrappedBuffer(value);
         buf.writerIndex(offset);
         buf.writeBytes(patch, 0, patch.length);
         return buf.array();
      }
      var buf = Unpooled.buffer(offset+patch.length);
      try {
      buf.writeBytes(value, 0, Math.min(value.length, offset));
      buf.writerIndex(offset);
      buf.writeBytes(patch, 0, patch.length);
      return buf.array();
      } catch (Throwable t) {
         buf.release();
         throw t;
      }
   }
}
