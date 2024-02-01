package org.infinispan.server.resp.commands.string;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

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

   private static byte[] setrange(byte[] value, byte[] patch, int offset) {
      if (value==null) {
         value = Util.EMPTY_BYTE_ARRAY;
      }

      if (patch.length+offset < value.length) {
         var buf = ByteBuffer.wrap(value);
         buf.put(offset, patch);
         return buf.array();
      }

      byte[] output = new byte[offset + patch.length];
      var buf = ByteBuffer.wrap(output);
      buf.put(value, 0, Math.min(value.length, offset));
      buf.put(offset, patch);
      return output;
   }
}
