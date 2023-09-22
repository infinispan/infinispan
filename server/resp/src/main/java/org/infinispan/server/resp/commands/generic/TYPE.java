package org.infinispan.server.resp.commands.generic;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespTypes;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TYPE Resp Command
 * <a href="https://redis.io/commands/type/">type</a>
 * @since 15.0
 */
public class TYPE extends RespCommand implements Resp3Command {
   public TYPE() {
      super(2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);
      MediaType vmt = handler.cache().getValueDataConversion().getStorageMediaType();
      return handler.stageToReturn(handler.cache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, vmt).getCacheEntryAsync(keyBytes).thenApply(e -> {
         if (e == null) {
            return RespTypes.none.name().getBytes(StandardCharsets.US_ASCII);
         }
         Class<?> c = e.getValue().getClass();
         return RespTypes.fromValueClass(c).name().getBytes(StandardCharsets.US_ASCII);
      }), ctx, Consumers.GET_BICONSUMER);
   }
}
