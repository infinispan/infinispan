package org.infinispan.server.resp.commands.tdigest;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TDIGEST.CREATE key [COMPRESSION compression]
 * <p>
 * Initializes a new t-digest sketch.
 *
 * @see <a href="https://redis.io/commands/tdigest.create/">TDIGEST.CREATE</a>
 * @since 16.2
 */
public class TDIGESTCREATE extends RespCommand implements Resp3Command {

   public TDIGESTCREATE() {
      super("TDIGEST.CREATE", -2, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int compression = TDigest.DEFAULT_COMPRESSION;

      // Parse optional COMPRESSION argument
      for (int i = 1; i < arguments.size(); i++) {
         String arg = new String(arguments.get(i)).toUpperCase();
         if ("COMPRESSION".equals(arg) && ((i + 1) < arguments.size())) {
            try {
               compression = Integer.parseInt(new String(arguments.get(++i)));
            } catch (NumberFormatException e) {
               handler.writer().customError("ERR invalid compression value");
               return handler.myStage();
            }
         }
      }

      if (compression <= 0) {
         handler.writer().customError("ERR compression must be positive");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TDigestCreateFunction function = new TDigestCreateFunction(compression);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
