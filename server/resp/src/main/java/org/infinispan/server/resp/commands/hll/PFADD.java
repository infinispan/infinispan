package org.infinispan.server.resp.commands.hll;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.hll.HyperLogLog;

import io.netty.channel.ChannelHandlerContext;

/**
 * The `<code>PFADD key [element [element ...]]</code>` command.
 * <p>
 * Adds an element to the HyperLogLog (HLL) structure. The command always verify if is possible to use the HLL structure
 * with the given <code>key</code>. If the command is invoked without elements, the entry is created.
 * </p>
 *
 * @see <a href="https://redis.io/commands/pfadd/">Redis documentation.</a>
 * @since 15.0
 * @author José Bolina
 */
public class PFADD extends RespCommand implements Resp3Command {

   public PFADD() {
      super(-2, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            ReadWriteMapImpl.create(FunctionalMapImpl.create(handler.typedCache(null)));

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

      return handler.stageToReturn(cs, ctx, (res, buf) -> {
         switch (res) {
            case NO_CHANGE:
               Consumers.LONG_BICONSUMER.accept(0L, buf);
               break;
            case ADDED:
               Consumers.LONG_BICONSUMER.accept(1L, buf);
               break;
            case NO_OP:
               RespErrorUtil.wrongType(handler.allocator());
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
