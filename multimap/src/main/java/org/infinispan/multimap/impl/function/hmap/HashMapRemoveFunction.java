package org.infinispan.multimap.impl.function.hmap;

import java.util.Collection;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_REMOVE_FUNCTION)
public class HashMapRemoveFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Integer> {

   private final Collection<HK> keys;

   public HashMapRemoveFunction(Collection<HK> keys) {
      this.keys = keys;
   }

   @ProtoFactory
   HashMapRemoveFunction(MarshallableCollection<HK> keys) {
      this.keys = MarshallableCollection.unwrap(keys);
   }

   @ProtoField(1)
   public MarshallableCollection<HK> getKeys() {
      return MarshallableCollection.create(keys);
   }

   @Override
   public Integer apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      if (existing.isEmpty()) return 0;

      HashMapBucket<HK, HV> bucket = existing.get();
      var res = bucket.removeAll(keys);

      if (res.bucket().isEmpty()) {
         view.remove();
      } else {
         view.set(res.bucket());
      }
      return res.response();
   }
}
