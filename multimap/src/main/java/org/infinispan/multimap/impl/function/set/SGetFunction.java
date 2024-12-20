package org.infinispan.multimap.impl.function.set;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by
 * {@link org.infinispan.multimap.impl.EmbeddedSetCache#get}
 * to get a Set value with the given key.
 *
 * @author Vittorio Rigamonti
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_S_GET_FUNCTION)
public final class SGetFunction<K, V> implements SetBucketBaseFunction<K, V, SetBucket<V>> {

   @Override
   public SetBucket<V> apply(EntryView.ReadWriteEntryView<K, SetBucket<V>> entryView) {
      Optional<SetBucket<V>> existing = entryView.peek();
      if (existing.isPresent()) {
         return existing.get();
      }
      return new SetBucket<V>();
   }
}
