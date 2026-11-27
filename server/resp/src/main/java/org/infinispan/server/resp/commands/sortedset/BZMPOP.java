package org.infinispan.server.resp.commands.sortedset;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.list.blocking.AbstractBlockingPop;
import org.infinispan.server.resp.commands.list.blocking.PopConfiguration;
import org.infinispan.server.resp.serialization.ResponseWriter;

/**
 * BZMPOP timeout numkeys key [key ...] &lt;MIN | MAX&gt; [COUNT count]
 * <p>
 * Blocking variant of {@link ZMPOP}: pops from the first non-empty sorted set, blocking until a member is available
 * or the timeout elapses.
 *
 * @see <a href="https://redis.io/commands/bzmpop/">BZMPOP</a>
 * @since 16.4
 */
public class BZMPOP extends AbstractBlockingPop<ZMPOP.PopResult> {

   private static final Source<ZMPOP.PopResult> SOURCE = new Source<>() {
      @Override
      public CompletionStage<ZMPOP.PopResult> poll(Resp3Handler handler, byte[] key, PopConfiguration configuration) {
         return handler.getSortedSeMultimap().pop(key, configuration.isHead(), configuration.count())
               .thenApply(c -> c == null || c.isEmpty() ? null : new ZMPOP.PopResult(key, c));
      }

      @Override
      public boolean isBucket(Object value) {
         return value instanceof SortedSetBucket;
      }

      @Override
      public BiConsumer<? super ZMPOP.PopResult, ResponseWriter> serializer() {
         return (res, writer) -> {
            if (res == null) {
               writer.nulls();
            } else {
               writer.write(res, res);
            }
         };
      }
   };

   public BZMPOP() {
      super(SOURCE, -5, 0, 0, 0,
            AclCategory.WRITE.mask() | AclCategory.SORTEDSET.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   protected PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments) {
      double timeout = ArgumentUtils.toDouble(arguments.get(0));
      if (timeout < 0) {
         handler.writer().mustBePositive();
         return null;
      }

      ZMPOP.Arguments args = ZMPOP.parseArguments(handler, arguments, 1);
      if (args == null) {
         return null;
      }
      return new PopConfiguration(args.min(), args.count(), (long) (timeout * Duration.ofSeconds(1).toMillis()), args.keys());
   }
}
