package org.infinispan.multimap.impl.function.hmap;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.KeyValuePair;

/**
 * Serializable function use by {@link org.infinispan.multimap.impl.EmbeddedMultimapPairCache#set(Object, Map.Entry[])}.
 * </p>
 * This function inserts a collection of key-value pairs into the multimap. If it not exists, a new one is created.
 *
 * @author José Bolina
 * @see <a href="https://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_HASH_MAP_PUT_FUNCTION)
public class HashMapPutFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Integer> {

   private final Collection<Map.Entry<HK, HV>> entries;
   private final boolean putIfAbsent;

   public HashMapPutFunction(Collection<Map.Entry<HK, HV>> entries) {
      this(entries, false);
   }

   public HashMapPutFunction(Collection<Map.Entry<HK, HV>> entries, boolean putIfAbsent) {
      this.entries = entries;
      this.putIfAbsent = putIfAbsent;
   }

   @ProtoFactory
   HashMapPutFunction(Stream<KeyValuePair<HK, HV>> entries, boolean putIfAbsent) {
      this.entries = entries.collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue)).entrySet();
      this.putIfAbsent = putIfAbsent;
   }

   @ProtoField(1)
   Stream<KeyValuePair<HK, HV>> getEntries() {
      return entries.stream().map(e -> new KeyValuePair<>(e.getKey(), e.getValue()));
   }

   @ProtoField(2)
   boolean isPutIfAbsent() {
      return putIfAbsent;
   }

   @Override
   public Integer apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Map<HK, HV> values = entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      Optional<HashMapBucket<HK, HV>> existing = view.peek();

      if (existing.isEmpty()) {
         view.set(HashMapBucket.create(values));
         return values.size();
      }
      HashMapBucket<HK, HV> bucket = existing.get();
      HashMapBucket.HashMapBucketResponse<Integer, HK, HV> res = putIfAbsent
            ? bucket.putIfAbsent(values)
            : bucket.putAll(values);
      view.set(res.bucket());

      return res.response();
   }
}
