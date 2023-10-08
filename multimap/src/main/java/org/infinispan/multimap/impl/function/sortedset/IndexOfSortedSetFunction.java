package org.infinispan.multimap.impl.function.sortedset;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache#indexOf(Object, Object, boolean)}
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_INDEX_OF_SORTED_SET_FUNCTION)
public final class IndexOfSortedSetFunction<K, V> implements SortedSetBucketBaseFunction<K, V, SortedSetBucket.IndexValue> {

   private final V member;
   private final boolean isRev;

   public IndexOfSortedSetFunction(V member, boolean isRev) {
      this.member = member;
      this.isRev = isRev;
   }

   @ProtoFactory
   IndexOfSortedSetFunction(MarshallableObject<V> member, boolean rev) {
      this(MarshallableObject.unwrap(member), rev);
   }

   @ProtoField(1)
   MarshallableObject<V> getMember() {
      return MarshallableObject.create(member);
   }

   @ProtoField(2)
   boolean isRev() {
      return isRev;
   }

   @Override
   public SortedSetBucket.IndexValue apply(EntryView.ReadWriteEntryView<K, SortedSetBucket<V>> entryView) {
      Optional<SortedSetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().indexOf(member, isRev);
      }
      return null;
   }
}
