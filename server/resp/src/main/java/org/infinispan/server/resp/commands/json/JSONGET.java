package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.GET
 *
 * @see <a href="https://redis.io/commands/json.get/">JSON.GET</a>
 * @since 15.1
 */
public class JSONGET extends RespCommand implements Resp3Command {

   public JSONGET() {
      super("JSON.GET", -2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      FunctionalMap.ReadOnlyMap<byte[], Object> cache = ReadOnlyMapImpl
            .create(FunctionalMapImpl.create(handler.typedCache(null)));
            CompletionStage<byte[]> cs = cache.eval(key, view -> {
               return (byte[])view.find().orElse(null);
            });
      return handler.stageToReturn(cs, ctx, ResponseWriter.BULK_STRING_BYTES);
   }

}
