package org.infinispan.server.resp.commands.set;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SINTER
 *
 * @see <a href="https://redis.io/commands/sinter/">SINTER</a>
 * @since 15.0
 */
public class SINTER extends RespCommand implements Resp3Command {

   public SINTER() {
      super(-2, 1, -1, 1, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      // Wrapping to exclude duplicate keys
      var uniqueKeys = getUniqueKeys(handler, arguments);
      var allEntries= esc.getAll(uniqueKeys).thenApply( sets -> SINTER.checkTypeOrEmpty(sets,uniqueKeys.size()));
      return handler.stageToReturn(
            allEntries.thenApply((sets) -> intersect(sets.values(), 0)),
            ctx, ResponseWriter.SET_BULK_STRING);
   }

   public static Set<byte[]> getUniqueKeys(Resp3Handler handler, List<byte[]> arguments) {
      var wrappedArgs = arguments.stream().map(WrappedByteArray::new).collect(Collectors.toSet());
      return wrappedArgs.stream().map(WrappedByteArray::getBytes).collect(Collectors.toSet());
   }

   public static Map<byte[], SetBucket<byte[]>> checkTypeOrEmpty(Map<byte[], SetBucket<byte[]>> m, int count) {
      boolean empty = false;
      // Iterate over all elements first to check wrong types
      for (SetBucket<byte[]> it : m.values()) {
         empty |= it.isEmpty() || empty;
      }
      // If any set is not found shortcut returning empty map
      if ((m.size() < count) || empty ) {
         return Collections.emptyMap();
      }
      return m;
   }

   public static Set<byte[]> checkTypesAndReturnEmpty(Collection<SetBucket<byte[]>> buckets) {
      var iter = buckets.iterator();
      // access all items to check for error
      while (iter.hasNext()) {
         var aSet = iter.next();
      }
      return Collections.emptySet();
   }

   public static Set<byte[]> intersect(Collection<SetBucket<byte[]>> buckets,
         int limit) {
      var iter = buckets.iterator();

      // Return empty set if null or empty
      if (!iter.hasNext()) {
         return Collections.emptySet();
      }
      var minSet = iter.next();
      if (minSet.isEmpty()) {
         return Collections.emptySet();
      }

      // Find the smallest set in the sets list
      while (iter.hasNext() && !minSet.isEmpty()) {
         var el = iter.next();
         minSet = minSet.size() <= el.size() ? minSet : el;
      }

      // Build a set with all the elements in minSet and in all the rest of the sets
      // up to limit if non zero
      Set<byte[]> result = new HashSet<>();
      for (var el : minSet.toList()) {
         if (buckets.stream().allMatch(set -> set.contains(el))) {
            result.add(el);
            if (limit > 0 && result.size() >= limit) {
               break;
            }
         }
      }
      return result;
   }
}
