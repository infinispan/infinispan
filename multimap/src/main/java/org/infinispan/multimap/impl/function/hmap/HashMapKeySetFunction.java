package org.infinispan.multimap.impl.function.hmap;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapPairCache#keySet(Object)}.
 * <p>
 * This functions returns the key set of the {@link HashMapBucket}, or an empty set if the entry is not found.
 * </p>
 *
 * @param <K>: Key type to identify the {@link HashMapBucket}.
 * @param <HK>: The {@link HashMapBucket} key type.
 * @param <HV>: The {@link HashMapBucket} value type.
 * @since 15.0
 * @see BaseFunction
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_KEY_SET_FUNCTION)
public class HashMapKeySetFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Set<HK>> {

   @Override
   public Set<HK> apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      if (existing.isPresent()) {
         return existing.get().keySet();
      }
      return Collections.emptySet();
   }
}
