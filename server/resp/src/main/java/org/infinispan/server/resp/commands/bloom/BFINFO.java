package org.infinispan.server.resp.commands.bloom;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.bloom.BloomFilterInfoFunction.BloomFilterInfo;
import org.infinispan.server.resp.commands.bloom.BloomFilterInfoFunction.InfoType;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
 * <p>
 * Returns information about a Bloom filter.
 *
 * @see <a href="https://redis.io/commands/bf.info/">BF.INFO</a>
 * @since 16.2
 */
public class BFINFO extends RespCommand implements Resp3Command {

   public BFINFO() {
      super("BF.INFO", -2, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      InfoType infoType = InfoType.ALL;

      if (arguments.size() == 2) {
         String type = new String(arguments.get(1), StandardCharsets.US_ASCII).toUpperCase();
         try {
            infoType = InfoType.valueOf(type);
         } catch (IllegalArgumentException e) {
            handler.writer().customError("ERR Invalid INFO argument");
            return handler.myStage();
         }
      }

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      BloomFilterInfoFunction function = new BloomFilterInfoFunction(infoType);
      CompletionStage<BloomFilterInfo> result = cache.eval(key, function);

      InfoType finalInfoType = infoType;
      return handler.stageToReturn(result, ctx, (info, w) -> {
         Map<String, Long> map = info.toMap();
         if (finalInfoType == InfoType.ALL) {
            w.arrayStart(map.size() * 2);
            for (Map.Entry<String, Long> entry : map.entrySet()) {
               w.simpleString(entry.getKey());
               w.integers(entry.getValue());
            }
            w.arrayEnd();
         } else {
            w.array(map.values().stream().toList(), Resp3Type.INTEGER);
         }
      });
   }
}
