package org.infinispan.server.resp.commands.countmin;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.countmin.CmsInfoFunction.CmsInfo;

import io.netty.channel.ChannelHandlerContext;

/**
 * CMS.INFO key
 * <p>
 * Returns width, depth and total count of the sketch.
 *
 * @see <a href="https://redis.io/commands/cms.info/">CMS.INFO</a>
 * @since 16.2
 */
public class CMSINFO extends RespCommand implements Resp3Command {

   public CMSINFO() {
      super("CMS.INFO", 2, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() != 1) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CompletionStage<CmsInfo> result = cache.eval(key, CmsInfoFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (info, w) -> {
         Map<String, Long> map = info.toMap();
         w.arrayStart(map.size() * 2);
         for (Map.Entry<String, Long> entry : map.entrySet()) {
            w.simpleString(entry.getKey());
            w.integers(entry.getValue());
         }
         w.arrayEnd();
      });
   }
}
