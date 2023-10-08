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
 * {@link org.infinispan.multimap.impl.EmbeddedMultimapListCache#rotate(Object, boolean)} (Object, boolean)}
 * to remove an element.
 *
 * @author Katia Aresti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_ROTATE_FUNCTION)
public final class RotateFunction<K, V> implements ListBucketBaseFunction<K, V, V> {

   @ProtoField(value = 1, defaultValue = "-1")
   final boolean rotateRight;

   @ProtoFactory
   public RotateFunction(boolean rotateRight) {
      this.rotateRight = rotateRight;
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, ListBucket<V>> entryView) {
      Optional<ListBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         ListBucket.ListBucketResult<V, V> result = existing.get().rotate(rotateRight);
         entryView.set(result.bucket());
         return result.result();
      }
      // key does not exist
      return null;
   }
}
