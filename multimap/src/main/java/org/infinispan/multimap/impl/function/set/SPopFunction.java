package org.infinispan.multimap.impl.function.set;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#remove}
 * to remove elements to a Set.
 *
 * @author Vittorio Rigamonti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of
 *      Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_POP_FUNCTION)
public final class SPopFunction<K, V> implements SetBucketBaseFunction<K, V, Collection<V>> {

   @ProtoField(1)
   final long count;

   @ProtoField(2)
   final boolean remove;

   @ProtoFactory
   public SPopFunction(long count, boolean remove) {
      this.count = count;
      this.remove = remove;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      if (count == 0 || existing.isEmpty()) {
         return Collections.emptyList();
      }
      var s = existing.get();
      if (count > 0) {
         var popped = getRandomSubset(s.toList(), count);
         if (remove) {
            var result = s.removeAll(popped);
            s = result.bucket();

            if (s.isEmpty()) {
               entryView.remove();
            } else {
               entryView.set(s);
            }
         }

         return popped;
      }
      var list = s.toList();
      return ThreadLocalRandom.current().ints(-count, 0, s.size()).mapToObj(list::get)
            .collect(Collectors.toList());
   }

   public static <T> Collection<T> getRandomSubset(List<T> list, long count) {
      if (list.size() <= count) {
         return list;
      } else {
         Collections.shuffle(list);
         return list.stream().limit(count).collect(Collectors.toList());
      }
   }
}
