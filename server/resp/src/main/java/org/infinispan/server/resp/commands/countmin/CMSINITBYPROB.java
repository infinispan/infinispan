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
 * CMS.INITBYPROB key error probability
 * <p>
 * Initializes a Count-Min Sketch to accommodate requested tolerances.
 *
 * @see <a href="https://redis.io/commands/cms.initbyprob/">CMS.INITBYPROB</a>
 * @since 16.2
 */
public class CMSINITBYPROB extends RespCommand implements Resp3Command {

   public CMSINITBYPROB() {
      super("CMS.INITBYPROB", 4, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      double error;
      double probability;

      try {
         error = Double.parseDouble(new String(arguments.get(1)));
         probability = Double.parseDouble(new String(arguments.get(2)));
      } catch (NumberFormatException e) {
         handler.writer().customError("ERR invalid error or probability");
         return handler.myStage();
      }

      if (error <= 0 || error >= 1 || probability <= 0 || probability >= 1) {
         handler.writer().customError("ERR error and probability must be between 0 and 1");
         return handler.myStage();
      }

      // Calculate width and depth from error and probability
      int width = (int) Math.ceil(2.0 / error);
      int depth = (int) Math.ceil(Math.log(1.0 / probability));

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CmsInitFunction function = new CmsInitFunction(width, depth);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
