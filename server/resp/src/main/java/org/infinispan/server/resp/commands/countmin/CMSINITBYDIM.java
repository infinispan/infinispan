package org.infinispan.server.resp.commands.countmin;

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
 * CMS.INITBYDIM key width depth
 * <p>
 * Initializes a Count-Min Sketch to dimensions specified by user.
 *
 * @see <a href="https://redis.io/commands/cms.initbydim/">CMS.INITBYDIM</a>
 * @since 16.2
 */
public class CMSINITBYDIM extends RespCommand implements Resp3Command {

   public CMSINITBYDIM() {
      super("CMS.INITBYDIM", 4, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int width;
      int depth;

      try {
         width = Integer.parseInt(new String(arguments.get(1)));
         depth = Integer.parseInt(new String(arguments.get(2)));
      } catch (NumberFormatException e) {
         handler.writer().customError("ERR invalid width or depth");
         return handler.myStage();
      }

      if (width <= 0 || depth <= 0) {
         handler.writer().customError("ERR width and depth must be positive");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CmsInitFunction function = new CmsInitFunction(width, depth);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
