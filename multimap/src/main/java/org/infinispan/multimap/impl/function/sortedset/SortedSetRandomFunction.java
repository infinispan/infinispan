package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collections;
import java.util.List;
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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#randomMembers(Object, int)} )}
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_RANDOM_FUNCTION)
public final class SortedSetRandomFunction<K, V> implements SortedSetBucketBaseFunction<K, V, List<ScoredValue<V>>> {

   @ProtoField(value = 1, defaultValue = "-1")
   final int count;

   @ProtoFactory
   public SortedSetRandomFunction(int count) {
      this.count = count;
   }

   @Override
   public List<ScoredValue<V>> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (count == 0 || existing.isEmpty()) {
         return Collections.emptyList();
      }
      return existing.get().randomMembers(count);
   }
}
