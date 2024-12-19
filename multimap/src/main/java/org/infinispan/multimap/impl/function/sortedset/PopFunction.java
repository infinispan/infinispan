package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ScoredValue;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#pop(Object, boolean, long)}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_POP_FUNCTION)
public final class PopFunction<K, V> implements SortedSetBucketBaseFunction<K, V, Collection<ScoredValue<V>>> {

   @ProtoField(value = 1, defaultValue = "false")
   final boolean min;

   @ProtoField(value = 2, defaultValue = "-1")
   final long count;

   @ProtoFactory
   public PopFunction(boolean min, long count) {
      this.min = min;
      this.count = count;
   }

   @Override
   public Collection<ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         SortedSetBucket<V> sortedSetBucket = existing.get();
         var result = sortedSetBucket.pop(min, count);
         Collection<ScoredValue<V>> poppedValues = result.result();
         if (result.bucket().size() == 0) {
            entryView.remove();
         } else {
            entryView.set(result.bucket());
         }
         return poppedValues;
      }
      return Collections.emptyList();
   }
}
