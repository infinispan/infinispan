package org.infinispan.server.resp.commands.hll;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.hll.HyperLogLog;

import io.netty.channel.ChannelHandlerContext;

/**
 * PFADD
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/pfadd/">PFADD</a>
 * @since 15.0
 */
public class PFADD extends RespCommand implements Resp3Command {

   public PFADD() {
      super(-2, 1, 1, 1, AclCategory.WRITE.mask() | AclCategory.HYPERLOGLOG.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      FunctionalMap.ReadWriteMap<byte[], Object> cache = FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      List<byte[]> elements = new ArrayList<>(arguments.subList(1, arguments.size()));
      CompletionStage<UpdateStatus> cs = cache.eval(key, view -> {
         HyperLogLog hll = parseHLL(view.find().orElse(null));

         // Storing something which is not an HLL.
         if (hll == null) return UpdateStatus.NO_OP.ordinal();

         boolean changed = view.peek().isEmpty();
         for (byte[] el : elements) {
            if (hll.add(el)) changed = true;
         }

         if (!changed) {
            return UpdateStatus.NO_CHANGE.ordinal();
         }

         view.set(hll);
         return UpdateStatus.ADDED.ordinal();
      }).thenApply(UpdateStatus::fromOrdinal);

      return handler.stageToReturn(cs, ctx, (res, writer) -> {
         switch (res) {
            case NO_CHANGE:
               writer.integers(0L);
               break;
            case ADDED:
               writer.integers(1L);
               break;
            case NO_OP:
               writer.wrongType();
               break;
         }
      });
   }

   private static HyperLogLog parseHLL(Object stored) {
      if (stored == null)
         return new HyperLogLog();

      if (stored instanceof HyperLogLog)
         return (HyperLogLog) stored;

      return null;
   }

   private enum UpdateStatus {
      NO_OP,
      NO_CHANGE,
      ADDED;

      private static final UpdateStatus[] values = UpdateStatus.values();

      public static UpdateStatus fromOrdinal(int ordinal) {
         return values[ordinal];
      }
   }
}
