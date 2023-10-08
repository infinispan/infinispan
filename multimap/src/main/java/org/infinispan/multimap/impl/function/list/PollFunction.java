package org.infinispan.multimap.impl.function.list;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#pollFirst(K, long)} and
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#pollLast(K, long)}
 * to poll N values at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_POLL_FUNCTION)
public final class PollFunction<K, V> implements ListBucketBaseFunction<K, V, Collection<V>> {

   @ProtoField(number = 1, defaultValue = "false")
   final boolean first;

   @ProtoField(value = 2, defaultValue = "-1")
   final long count;

   @ProtoFactory
   public PollFunction(boolean first, long count) {
      this.first = first;
      this.count = count;
   }

   @Override
   public Collection<V> apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         if (count == 0) {
            // Do nothing and return an empty list
            return List.of();
         }

         ListBucket.ListBucketResult<Collection<V>, V> result = existing.get().poll(first, count);
         if (result.bucket().isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(result.bucket());
         }
         return result.result();
      }
      // key does not exist
      return null;
   }
}
