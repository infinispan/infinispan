package org.infinispan.server.resp.commands.set;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

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

   public SINTER() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      // Wrapping to exclude duplicate keys
      var uniqueKeys = getUniqueKeys(handler, arguments);
      var allEntries= esc.getAll(uniqueKeys);
      return handler.stageToReturn(
            allEntries
                  .thenApply((sets) -> sets.size()==uniqueKeys.size() ? intersect(sets.values(), 0) : checkTypesAndReturnEmpty(sets.values())),
            ctx, Consumers.COLLECTION_BULK_BICONSUMER);
   }

   public static Set<byte[]> getUniqueKeys(Resp3Handler handler, List<byte[]> arguments) {
      var wrappedArgs = arguments.stream().map(WrappedByteArray::new).collect(Collectors.toSet());
      return wrappedArgs.stream().map(WrappedByteArray::getBytes).collect(Collectors.toSet());
   }

   public static List<byte[]> checkTypesAndReturnEmpty(Collection<SetBucket<byte[]>> buckets) {
      var iter = buckets.iterator();
      // access all items to check for error
      while (iter.hasNext()) {
      var aSet = iter.next();
      }
      return Collections.emptyList();
   }

   public static List<byte[]> intersect(Collection<SetBucket<byte[]>> buckets,
         int limit) {
      var iter = buckets.iterator();

      // Return empty set if null or empty
      if (!iter.hasNext()) {
         return Collections.emptyList();
      }
      var minSet = iter.next();
      if (minSet.isEmpty()) {
         return Collections.emptyList();
      }

      // Find the smallest set in the sets list
      while (iter.hasNext() && !minSet.isEmpty()) {
         var el = iter.next();
         minSet = minSet.size() <= el.size() ? minSet : el;
      }

      // Build a set with all the elements in minSet and in all the rest of the sets
      // up to limit if non zero
      var result = new ArrayList<byte[]>();
      for (var el : minSet.toList()) {
         if (!buckets.stream().anyMatch(set -> !set.contains(el))) {
            result.add(el);
            if (limit > 0 && result.size() >= limit) {
               break;
            }
         }
      }
      return result;
   }
}
