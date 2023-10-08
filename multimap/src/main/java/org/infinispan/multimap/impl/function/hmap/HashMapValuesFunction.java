package org.infinispan.multimap.impl.function.hmap;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link org.infinispan.multimap.impl.EmbeddedMultimapPairCache#values(Object)}.
 * <p>
 *    This function returns the values of the {@link HashMapBucket}, or an empty set if the entry is not found.
 * </p>
 *
 * @param <K>: Key type to identify the {@link HashMapBucket}.
 * @param <HK>: The {@link HashMapBucket} key type.
 * @param <HV>: The {@link HashMapBucket} value type.
 * @since 15.0
 * @see BaseFunction
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_VALUES_FUNCTION)
public class HashMapValuesFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Collection<HV>> {

   static final HashMapValuesFunction<?, ?, ?> INSTANCE = new HashMapValuesFunction<>();

   private HashMapValuesFunction() {}

   @ProtoFactory
   @SuppressWarnings("unchecked")
   public static <K, HK, HV> HashMapValuesFunction<K, HK, HV> instance() {
      return (HashMapValuesFunction<K, HK, HV>) INSTANCE;
   }

   @Override
   public Collection<HV> apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      if (existing.isPresent()) {
         return existing.get().values();
      }
      return Collections.emptyList();
   }
}
