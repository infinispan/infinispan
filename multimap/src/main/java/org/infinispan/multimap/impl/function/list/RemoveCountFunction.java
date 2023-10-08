package org.infinispan.multimap.impl.function.list;

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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#remove(Object, long, Object)}
 * to remove an element.
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_REMOVE_COUNT_FUNCTION)
public final class RemoveCountFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {

   @ProtoField(1)
   final long count;
   private final V element;

   public RemoveCountFunction(long count, V element) {
      this.count = count;
      this.element = element;
   }

   @ProtoFactory
   RemoveCountFunction(long count, MarshallableObject<V> element) {
      this(count, MarshallableObject.unwrap(element));
   }

   @ProtoField(2)
   MarshallableObject<V> getElement() {
      return MarshallableObject.create(element);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> prevBucket = existing.get();
         ListBucket.ListBucketResult<Long, V> result = prevBucket.remove(count, element);
         if (result.bucket().isEmpty()) {
            // if the list is empty, remove
            entryView.remove();
            return result.result();
         }

         entryView.set(result.bucket());
         return result.result();
      }
      // key does not exist
      return 0L;
   }
}
