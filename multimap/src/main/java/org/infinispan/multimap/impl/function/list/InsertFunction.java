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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#insert(Object, boolean, Object, Object)}
 * to insert an element before or after the provided pivot element.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_INSERT_FUNCTION)
public final class InsertFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {

   private final boolean before;
   private final V pivot;
   private final V element;

   public InsertFunction(boolean before, V pivot, V element) {
      this.before = before;
      this.pivot = pivot;
      this.element = element;
   }

   @ProtoFactory
   InsertFunction(boolean before, MarshallableObject<V> pivot, MarshallableObject<V> element) {
      this(before, MarshallableObject.unwrap(pivot), MarshallableObject.unwrap(element));
   }

   @ProtoField(value = 1, defaultValue = "false")
   boolean isBefore() {
      return before;
   }

   @ProtoField(2)
   MarshallableObject<V> getPivot() {
      return MarshallableObject.create(pivot);
   }

   @ProtoField(3)
   MarshallableObject<V> getElement() {
      return MarshallableObject.create(element);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> bucket = existing.get().insert(before, pivot, element);
         if (bucket == null) {
            // pivot not found
            return -1L;
         } else {
            entryView.set(bucket);
            return (long) bucket.size();
         }

      }
      // key does not exist
      return 0L;
   }
}
