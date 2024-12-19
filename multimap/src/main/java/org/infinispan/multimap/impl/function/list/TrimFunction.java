package org.infinispan.multimap.impl.function.list;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#trim(Object, long, long)}
 * to trim the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_TRIM_FUNCTION)
public final class TrimFunction<K, V> implements ListBucketBaseFunction<K, V, Boolean> {

   @ProtoField(number = 1, defaultValue = "-1")
   final long from;

   @ProtoField(number = 2, defaultValue = "-1")
   final long to;

   @ProtoFactory
   public TrimFunction(long from, long to) {
      this.from = from;
      this.to = to;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket<V> bucket = existing.get();
         ListBucket<V> trimmed = bucket.trim(from, to);
         if (trimmed.isEmpty()) {
            entryView.remove();
         } else {
            entryView.set(trimmed);
         }
         return true;
      }
      // key does not exist
      return false;
   }
}
