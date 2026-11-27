package org.infinispan.server.resp.commands.sortedset.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.list.blocking.PopConfiguration;
import org.infinispan.server.resp.commands.list.blocking.SingleBlockingPop;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;

/**
 * Common implementation for the BZPOPMIN and BZPOPMAX commands.
 */
public abstract class BZPOP extends SingleBlockingPop<BZPOP.PopResult> {

   private static final Source<PopResult> SOURCE = new Source<>() {
      @Override
      public CompletionStage<PopResult> poll(Resp3Handler handler, byte[] key, PopConfiguration configuration) {
         return handler.getSortedSeMultimap().pop(key, configuration.isHead(), 1)
               .thenApply(c -> c == null || c.isEmpty() ? null : new PopResult(key, c.iterator().next()));
      }

      @Override
      public boolean isBucket(Object value) {
         return value instanceof SortedSetBucket;
      }

      @Override
      public BiConsumer<? super PopResult, ResponseWriter> serializer() {
         return (res, writer) -> {
            if (res == null) {
               writer.nulls();
            } else {
               writer.write(res, res);
            }
         };
      }
   };

   protected BZPOP(boolean min, long aclMask) {
      super(SOURCE, min, -3, 1, -2, 1, aclMask);
   }

   /**
    * The key, the popped member and its score.
    */
   public record PopResult(byte[] key, ScoredValue<byte[]> value) implements JavaObjectSerializer<PopResult> {
      @Override
      public void accept(PopResult res, ResponseWriter writer) {
         writer.array(List.of(key, value.getValue(), value.score()), (o, w) -> {
            if (o instanceof Number n) {
               w.doubles(n);
            } else {
               w.string((byte[]) o);
            }
         });
      }
   }
}
