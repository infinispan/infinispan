package org.infinispan.server.resp.commands.set;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelHandlerContext;

/**
 * {@link} https://redis.io/commands/sinter/
 *
 * Returns the members of the set resulting from the intersection of all the
 * given sets.
 *
 * @since 15.0
 */
public class SINTER extends RespCommand implements Resp3Command {
   static Set<WrappedByteArray> EMPTY_SET = new HashSet<>();

   public SINTER() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {

      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      var sets = SINTER.aggregateSets(handler, arguments, acs);
      return handler.stageToReturn(
            acs.freeze()
                  .thenApply((v) -> intersect(sets, 0)),
            ctx, Consumers.COLLECTION_BULK_BICONSUMER);
   }

   public static Collection<CompletionStage<Set<WrappedByteArray>>> aggregateSets(Resp3Handler handler,
         List<byte[]> keys, AggregateCompletionStage<Void> acs) {
      EmbeddedSetCache<byte[], WrappedByteArray> esc = handler.getEmbeddedSetCache();
      var setList = new ArrayList<CompletionStage<Set<WrappedByteArray>>>(keys.size());
      for (int i = 0; i < keys.size(); i++) {
         var cs = esc.get(keys.get(i)).thenApply(r -> (r != null ? r : EMPTY_SET));
         setList.add(cs);
         acs.dependsOn(cs);
      }
      return setList;
   }

   public static Set<WrappedByteArray> intersect(Collection<CompletionStage<Set<WrappedByteArray>>> setOfStages,
         int limit) {
      // Extract values from stages
      var setOfSets = setOfStages.stream().map(cs -> cs.toCompletableFuture().getNow(null)).collect(Collectors.toSet());
      var iter = setOfSets.iterator();

      // Return empty set if null or empty
      if (!iter.hasNext()) {
         return EMPTY_SET;
      }
      var minSet = iter.next();
      if (minSet.isEmpty()) {
         return EMPTY_SET;
      }

      // Find the smallest set in the sets list
      while (iter.hasNext() && !minSet.isEmpty()) {
         var el = iter.next();
         minSet = minSet.size() <= el.size() ? minSet : el;
      }

      // Build a set with all the elements in minSet and in all the rest of the sets
      // up to limit if non zero
      var result = new HashSet<WrappedByteArray>();
      for (var el : minSet) {
         if (!setOfSets.stream().anyMatch(set -> !set.contains(el))) {
            result.add(el);
            if (limit > 0 && result.size() >= limit) {
               break;
            }
         }
      }
      return result;
   }
}
