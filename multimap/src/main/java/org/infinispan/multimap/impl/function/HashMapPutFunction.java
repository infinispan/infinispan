package org.infinispan.multimap.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.internal.MultimapDataConverter;

/**
 * Serializable function use by {@link org.infinispan.multimap.impl.EmbeddedMultimapPairCache#set(Object, Map.Entry[])}.
 * </p>
 * This function inserts a collection of key-value pairs into the multimap. If it not exists, a new one is created.
 *
 * @author José Bolina
 * @see <a href="http://infinispan.org/documentation/">Marshalling of Functions</a>
 * @since 15.0
 */
public class HashMapPutFunction<K, HK, HV> extends HashMapBucketBaseFunction<K, HK, HV, Integer> {
   public static final AdvancedExternalizer<HashMapPutFunction> EXTERNALIZER = new Externalizer();

   private final Collection<Map.Entry<HK, HV>> entries;

   public HashMapPutFunction(MultimapDataConverter<HK, HV> converter, Collection<Map.Entry<HK, HV>> entries) {
      super(converter);
      this.entries = entries;
   }

   @Override
   public Integer apply(EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>> view) {
      Map<HK, HV> values = entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      Optional<HashMapBucket<HK, HV>> existing = view.peek();

      int res;
      HashMapBucket<HK, HV> bucket;
      if (existing.isPresent()) {
         bucket = existing.get();
         res = bucket.putAll(values, converter);
      } else {
         bucket = HashMapBucket.create(values, converter);
         res = values.size();
      }
      view.set(bucket);

      return res;
   }

   public static class Externalizer implements AdvancedExternalizer<HashMapPutFunction> {

      @Override
      public Set<Class<? extends HashMapPutFunction>> getTypeClasses() {
         return Collections.singleton(HashMapPutFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.HASH_MAP_PUT_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, HashMapPutFunction object) throws IOException {
         output.writeObject(object.converter);
         output.writeInt(object.entries.size());
         Collection<Map.Entry> e = object.entries;
         for (Map.Entry entry : e) {
            output.writeObject(entry.getKey());
            output.writeObject(entry.getValue());
         }
      }

      @Override
      public HashMapPutFunction<?, ?, ?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         MultimapDataConverter converter = (MultimapDataConverter) input.readObject();
         int size = input.readInt();
         Map<Object, Object> values = new HashMap<>(size);
         for (int i = 0; i < size; i++) {
            values.put(input.readObject(), input.readObject());
         }
         return new HashMapPutFunction<>(converter, values.entrySet());
      }
   }
}
