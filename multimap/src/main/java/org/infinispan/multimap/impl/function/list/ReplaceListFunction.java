package org.infinispan.multimap.impl.function.list;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableDeque;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#replace(Object, List)}
 *
 * @author Katia Aresti
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_REPLACE_LIST_FUNCTION)
public final class ReplaceListFunction<K, V> implements ListBucketBaseFunction<K, V, Long> {

   private final Deque<V> values;

   public ReplaceListFunction(List<V> values) {
      this(ReplaceListFunction.get(values));
   }

   public ReplaceListFunction(Deque<V> values) {
      this.values = values;
   }

   @ProtoFactory
   ReplaceListFunction(MarshallableDeque<V> values) {
      this.values = MarshallableDeque.unwrap(values);
   }

   @ProtoField(1)
   MarshallableDeque<V> getValues() {
      return MarshallableDeque.create(values);
   }

   @Override
   public Long apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      ListBucket<V> bucket = null;
      if (existing.isPresent()) {
        bucket = existing.get();
      } else if (values != null && !values.isEmpty()){
         bucket = new ListBucket<>();
      }

      if (bucket != null) {
         ListBucket<V> updated = bucket.replace(values);
         if (updated.isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(updated);
         }
         return updated.size();
      }

      // nothing has been done
      return 0L;
   }

   @SuppressWarnings("unchecked")
   private static <E> Deque<E> get(List<E> values) {
      if (values == null || values instanceof Deque<?> dq)
         return (Deque<E>) values;

      return new ArrayDeque<>(values);
   }
}
