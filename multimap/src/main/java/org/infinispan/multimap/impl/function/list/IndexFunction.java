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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#index(Object, long)}
 * to retrieve the index at the head or the tail of the multimap list value.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_INDEX_FUNCTION)
public final class IndexFunction<K, V> implements ListBucketBaseFunction<K, V, V> {

   @ProtoField(value = 1, defaultValue = "-1")
   final long index;

   @ProtoFactory
   public IndexFunction(long index) {
      this.index = index;
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get().index(index);
      }
      // key does not exist
      return null;
   }
}
