package org.infinispan.multimap.impl.function.list;

import java.util.Collection;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#indexOf(Object, Object, Long, Long, Long)}
 * to retrieve the index at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_INDEX_OF_FUNCTION)
public final class IndexOfFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<Long>> {

   @ProtoField(1)
   final long count;

   @ProtoField(2)
   final long rank;

   @ProtoField(3)
   final long maxLen;

   private final V element;

   public IndexOfFunction(V element, long count, long rank, long maxLen) {
      this.element = element;
      this.count = count;
      this.rank = rank;
      this.maxLen = maxLen;
   }

   @ProtoFactory
   IndexOfFunction(long count, long rank, long maxLen, MarshallableObject<V> element) {
      this(MarshallableObject.unwrap(element), count, rank, maxLen);
   }

   @ProtoField(4)
   MarshallableObject<V> getElement() {
      return MarshallableObject.create(element);
   }

   @Override
   public Collection<Long> apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().indexOf(element, count, rank, maxLen);
      }
      // key does not exist
      return null;
   }
}
