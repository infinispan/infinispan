package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.operation.CopyOperation;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * COPY
 *
 * @see <a href="https://redis.io/commands/copy/">COPY</a>
 * @since 16.1
 */
public class COPY extends RespCommand implements Resp3Command {

   public COPY() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.KEYSPACE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      if (arguments.size() < 2) {
         return CompletableFuture.failedFuture(new IllegalStateException("Missing arguments"));
      }

      return handler.stageToReturn(CopyOperation.perform(handler.cache(), handler.respServer().getCacheManager(), arguments),
            ctx, ResponseWriter.INTEGER);
   }
}
