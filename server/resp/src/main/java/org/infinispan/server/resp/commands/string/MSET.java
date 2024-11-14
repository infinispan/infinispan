package org.infinispan.server.resp.commands.string;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * MSET
 *
 * @see <a href="https://redis.io/commands/mset/">MSET</a>
 * @since 14.0
 */
public class MSET extends RespCommand implements Resp3Command {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   public MSET() {
      super(-3, 1, -1, 2);
   }

   @Override
   public long aclMask() {
      return AclCategory.WRITE | AclCategory.STRING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      int keyValuePairCount = arguments.size();
      if ((keyValuePairCount & 1) == 1) {
         log.tracef("Received: %s count for keys and values combined, should be even for MSET", keyValuePairCount);
         handler.writer().customError("Missing a value for a key");
         return handler.myStage();
      }
      AggregateCompletionStage<Void> setStage = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < keyValuePairCount; i += 2) {
         byte[] keyBytes = arguments.get(i);
         byte[] valueBytes = arguments.get(i + 1);
         setStage.dependsOn(handler.cache().putAsync(keyBytes, valueBytes));
      }
      return handler.stageToReturn(setStage.freeze(), ctx, ResponseWriter.OK);
   }
}
