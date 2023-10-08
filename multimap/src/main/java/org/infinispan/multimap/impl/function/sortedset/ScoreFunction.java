package org.infinispan.multimap.impl.function.sortedset;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#score(Object, Object)}.
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SCORE_FUNCTION)
public final class ScoreFunction<K, V> implements SortedSetBucketBaseFunction<K, V, List<Double>> {

   private final List<V> members;

   public ScoreFunction(List<V> members) {
      this.members = members;
   }

   @ProtoFactory
   ScoreFunction(MarshallableList<V> members) {
      this.members = MarshallableList.unwrap(members);
   }

   @ProtoField(1)
   MarshallableList<V> getMembers() {
      return MarshallableList.create(members);
   }

   @Override
   public List<Double> apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().scores(members);
      }
      return Collections.emptyList();
   }
}
