package org.infinispan.multimap.impl.function.hmap;

import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_REPLACE_FUNCTION)
public class HashMapReplaceFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Boolean> {

   private final HK property;
   private final HV expected;
   private final HV replacement;

   public HashMapReplaceFunction(HK property, HV expected, HV replacement) {
      this.property = property;
      this.expected = expected;
      this.replacement = replacement;
   }

   @ProtoFactory
   HashMapReplaceFunction(MarshallableObject<HK> property, MarshallableObject<HV> expected, MarshallableObject<HV> replacement) {
      this.property = MarshallableObject.unwrap(property);
      this.expected = MarshallableObject.unwrap(expected);
      this.replacement = MarshallableObject.unwrap(replacement);
   }

   @ProtoField(1)
   MarshallableObject<HK> getProperty() {
      return MarshallableObject.create(property);
   }

   @ProtoField(2)
   MarshallableObject<HV> getExpected() {
      return MarshallableObject.create(expected);
   }

   @ProtoField(3)
   MarshallableObject<HV> getReplacement() {
      return MarshallableObject.create(replacement);
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Optional<HashMapBucket<HK, HV>> existing = view.peek();
      HashMapBucket<HK, HV> bucket = existing.orElse(HashMapBucket.create(Map.of()));

      // Replace returns null when there are no changes.
      HashMapBucket<HK, HV> next = bucket.replace(property, expected, replacement);
      if (next != null && next != bucket) view.set(next);

      return next != null;
   }
}
