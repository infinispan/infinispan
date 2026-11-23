package org.infinispan.multimap.impl.function.hmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Serializable function used by {@link EmbeddedMultimapPairCache#getAndRemove(Object, List)}.
 * <p>
 * Removes the given keys from the hash map and returns the values they had, in the same order as the keys. Missing
 * keys map to {@code null}. The entry is removed when the hash map becomes empty.
 *
 * @param <K>  the type of the cache key
 * @param <HK> the type of the hash map keys
 * @param <HV> the type of the hash map values
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_GET_AND_REMOVE_FUNCTION)
public class HashMapGetAndRemoveFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, List<HV>> {

   private final Collection<HK> keys;

   public HashMapGetAndRemoveFunction(Collection<HK> keys) {
      this.keys = keys;
   }

   @ProtoFactory
   HashMapGetAndRemoveFunction(MarshallableCollection<HK> keys) {
      this.keys = MarshallableCollection.unwrap(keys);
   }

   @ProtoField(1)
   public MarshallableCollection<HK> getKeys() {
      return MarshallableCollection.create(keys);
   }

   @Override
   public List<HV> apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      if (existing.isEmpty()) {
         List<HV> missing = new ArrayList<>(keys.size());
         for (int i = 0; i < keys.size(); i++) {
            missing.add(null);
         }
         return missing;
      }

      var res = existing.get().getAndRemoveAll(keys);
      if (res.bucket().isEmpty()) {
         view.remove();
      } else {
         view.set(res.bucket());
      }
      return res.response();
   }
}
