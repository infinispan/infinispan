package org.infinispan.server.resp.commands;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.CacheException;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface Resp3Command {
   String CRLF = "\r\n";

   CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments);

   default CompletionStage<RespRequestHandler> handleException(Resp3Handler handler, Throwable t) {
      if (t instanceof CacheException) {
         CacheException cacheException = (CacheException) t;
         if (cacheException.getCause() instanceof ClassCastException) {
            RespErrorUtil.wrongType(handler.allocatorToUse());
            return handler.myStage();
         }
      }
      throw new RuntimeException(t);
   }
}
