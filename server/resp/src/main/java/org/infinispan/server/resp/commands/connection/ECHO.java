package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/echo/
 * @since 14.0
 */
public class ECHO extends RespCommand implements Resp3Command {
   public ECHO() {
      super(2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      byte[] argument = arguments.get(0);
      ByteBuf bufferToWrite = ByteBufferUtils.
            stringToByteBufWithExtra("$" + argument.length + CRLF_STRING, handler.allocator(), argument.length + 2);
      bufferToWrite.writeBytes(argument);
      bufferToWrite.writeByte('\r').writeByte('\n');
      return handler.myStage();
   }
}
