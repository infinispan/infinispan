package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.MGET
 *
 * @see <a href="https://redis.io/commands/json.mget/">JSON.MGET</a>
 * @since 15.2
 */
public class JSONMGET extends RespCommand implements Resp3Command {

   public JSONMGET() {
      super("JSON.MGET", -3, 1, -1, 1, AclCategory.JSON.mask() | AclCategory.READ.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      List<byte[]> keys = arguments.subList(0, arguments.size() - 1);
      // Getting path as list to match get() signature
      List<byte[]> paths = arguments.subList(arguments.size() - 1, arguments.size());

      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<List<byte[]>> results = CompletionStages.performSequentially(keys.iterator(),
            k -> ejc.get(k, paths, null, null, null).exceptionally(JSONMGET::handleWrongTypeError),
            Collectors.toList());
      return handler.stageToReturn(results, ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   private static byte[] handleWrongTypeError(Throwable ex) {
      if (RespUtil.isWrongTypeError(ex)) {
         return null;
      }
      throw CompletableFutures.asCompletionException(ex);
   }

}
